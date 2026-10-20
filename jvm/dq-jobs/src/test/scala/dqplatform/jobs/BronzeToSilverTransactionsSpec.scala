package dqplatform.jobs

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import dqplatform.client.{DQClient, DQConfig, ServiceApi, ServiceException}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Path, Paths}
import java.time.LocalDate

class BronzeToSilverTransactionsSpec extends AnyFunSuite with Matchers {

  private lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName(BronzeToSilverTransactions.AppName)
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    .getOrCreate()

  private val mapper = new ObjectMapper()
  private val suite = mapper.readTree(
    new String(Files.readAllBytes(Paths.get("../../suites/silver_transactions.json")), "UTF-8")).get("suite")

  private class FakeService(verdict: String, down: Boolean = false) extends ServiceApi {
    var posted: Option[JsonNode] = None
    override def getSuite(dataset: String): JsonNode = {
      if (down) throw new ServiceException("connection refused")
      mapper.createObjectNode().put("version", 1).set[JsonNode]("suite", suite)
    }
    override def postRun(payload: JsonNode): JsonNode = {
      posted = Some(payload)
      mapper.readTree(s"""{"verdict": "$verdict", "reasons": []}""")
    }
  }

  private def dq(service: ServiceApi) =
    new DQClient(spark, DQConfig("http://unused", "silver.transactions", "enforce", true, 10, "spec"), service)

  /** Bronze có replay (TX1 hai bản), 2 dòng thiếu khóa, 1 msisdn sai, msisdn đầu 0, status lẫn hoa thường. */
  private def bronze: DataFrame = {
    import spark.implicits._
    Seq[(String, String, String, String, String, String, String, String)](
      ("TX1", "SUB1", "0912345678", "10000", " success ", "HN", "2026-10-17 23:30:00", "2026-10-18 00:05:00"),
      ("TX1", "SUB1", "0912345678", "20000", "FAILED", "HN", "2026-10-17 23:30:00", "2026-10-18 02:00:00"),
      ("TX2", "SUB2", "84912345671", "10000", "SUCCESS", "HCM", "2026-10-17 10:00:00", "2026-10-18 00:01:00"),
      ("TX3", "SUB3", "12345", "10000", "SUCCESS", "DN", "2026-10-17 11:00:00", "2026-10-18 00:02:00"),
      (null, "SUB4", "84912345672", "10000", "SUCCESS", "HN", "2026-10-17 12:00:00", "2026-10-18 00:03:00"),
      (null, "SUB5", "84912345673", "10000", "SUCCESS", "HN", "2026-10-17 13:00:00", "2026-10-18 00:04:00")
    ).toDF("transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time", "ingest_time")
  }

  private def newLake(): Path = {
    val lake = Files.createTempDirectory("dq-lake")
    bronze.write.parquet(lake.resolve("bronze/transactions/dt=2026-10-17").toString)
    lake
  }

  private def args(lake: Path) = BronzeToSilverTransactions.Args(LocalDate.parse("2026-10-17"), lake.toString)

  test("transform: chuẩn hóa, dedup giữ bản ingest mới nhất, giữ dòng thiếu khóa; đếm số giao dịch nguồn") {
    val out = TransactionsTransform.transform(bronze, "2026-10-17")
    out.count() shouldBe 5 // TX1 (1 bản), TX2, TX3, 2 dòng null
    val tx1 = out.where("transaction_id = 'TX1'").head()
    tx1.getAs[String]("msisdn") shouldBe "84912345678"
    tx1.getAs[String]("status") shouldBe "FAILED" // bản ingest lúc 02:00 thắng
    out.where("transaction_id IS NULL").count() shouldBe 2
    out.columns should not contain "ingest_time"
    TransactionsTransform.sourceRowCount(bronze) shouldBe 5
  }

  test("verdict WARN: publish vào silver_scala, quarantine vào transactions_scala, exit 0") {
    val lake = newLake()
    val service = new FakeService("WARN")
    BronzeToSilverTransactions.run(spark, args(lake), dq(service)) shouldBe BronzeToSilverTransactions.ExitPublished

    val silver = spark.read.parquet(lake.resolve("silver_scala/transactions/dt=2026-10-17").toString)
    silver.columns should not contain "dt"
    silver.select("transaction_id").collect().map(_.getString(0)).sorted shouldBe Array("TX1", "TX2")

    val quarantine = spark.read.parquet(lake.resolve("quarantine/transactions_scala").toString)
    quarantine.count() shouldBe 3 // TX3 msisdn sai + 2 dòng thiếu transaction_id
    quarantine.where("_dq_run_id IS NULL").count() shouldBe 0
    Files.exists(lake.resolve("silver/transactions")) shouldBe false // không đụng thư mục của job Python

    val p = service.posted.get
    p.get("parameters").get("source_row_count").asText shouldBe "5"
    p.get("parameters").get("partition_end").asText shouldBe "2026-10-18 00:00:00"
  }

  test("chạy lại cùng dt: thay Silver, quarantine không nhân bản, không để lại staging/backup") {
    val lake = newLake()
    (1 to 2).foreach { _ =>
      BronzeToSilverTransactions.run(spark, args(lake), dq(new FakeService("WARN"))) shouldBe
        BronzeToSilverTransactions.ExitPublished
    }
    spark.read.parquet(lake.resolve("silver_scala/transactions/dt=2026-10-17").toString).count() shouldBe 2
    val quarantine = spark.read.parquet(lake.resolve("quarantine/transactions_scala").toString)
    quarantine.count() shouldBe 3
    quarantine.select("_dq_run_id").distinct().count() shouldBe 1 // chỉ còn dòng của lần chạy sau
    Files.list(lake.resolve("silver_scala/transactions")).toArray.map(_.toString.split("/").last) shouldBe
      Array("dt=2026-10-17")
    Files.list(lake.resolve("_staging/transactions_scala")).count() shouldBe 0
  }

  test("verdict FAIL: không publish, vẫn ghi quarantine, exit 3") {
    val lake = newLake()
    BronzeToSilverTransactions.run(spark, args(lake), dq(new FakeService("FAIL"))) shouldBe
      BronzeToSilverTransactions.ExitDqFailed
    Files.exists(lake.resolve("silver_scala/transactions/dt=2026-10-17")) shouldBe false
    Files.exists(lake.resolve("quarantine/transactions_scala")) shouldBe true
  }

  test("DQ service lỗi (fail-closed): exit 2, không ghi gì") {
    val lake = newLake()
    BronzeToSilverTransactions.run(spark, args(lake), dq(new FakeService("PASS", down = true))) shouldBe
      BronzeToSilverTransactions.ExitServiceDown
    Files.exists(lake.resolve("silver_scala")) shouldBe false
    Files.exists(lake.resolve("quarantine")) shouldBe false
  }

  test("đọc tham số dòng lệnh") {
    BronzeToSilverTransactions.Args.parse(Seq("--dt", "2026-10-17", "--lake", "hdfs://ns/lake")) shouldBe
      BronzeToSilverTransactions.Args(LocalDate.parse("2026-10-17"), "hdfs://ns/lake", "silver_scala")
    an[IllegalArgumentException] should be thrownBy BronzeToSilverTransactions.Args.parse(Seq("--lake", "/x"))
    an[IllegalArgumentException] should be thrownBy BronzeToSilverTransactions.Args.parse(Seq("--dt", "2026-10-17", "--oops", "1"))
  }
}
