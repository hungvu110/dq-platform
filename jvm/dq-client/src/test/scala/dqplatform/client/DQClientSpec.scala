package dqplatform.client

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit, to_timestamp}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

/** Chạy suite thật của repo qua DQClient với service giả: suite -> Deequ -> quarantine -> payload. */
class DQClientSpec extends AnyFunSuite with Matchers with SparkTestSession {

  private val mapper = new ObjectMapper()
  private val suiteFile = mapper.readTree(
    new String(Files.readAllBytes(Paths.get("../../suites/silver_transactions.json")), "UTF-8"))

  private class FakeService(verdict: String = "WARN", getFails: Boolean = false, postFails: Boolean = false,
                            failStatus: Option[Int] = None) extends ServiceApi {
    var posted: Option[JsonNode] = None

    override def getSuite(dataset: String): JsonNode = {
      if (getFails) throw new ServiceException(failStatus.fold("connection refused")(s => s"HTTP $s"), failStatus)
      val n = mapper.createObjectNode()
      n.put("dataset", dataset)
      n.put("version", 3)
      n.set[JsonNode]("suite", suiteFile.get("suite"))
      n
    }

    override def postRun(payload: JsonNode): JsonNode = {
      if (postFails) throw new ServiceException("HTTP 503")
      posted = Some(payload)
      mapper.readTree(s"""{"run_id": "x", "verdict": "$verdict", "reasons": ["[warn][completeness] r"],
        "dimensions": {"validity": {"evaluated": 6, "passed": 4}}}""")
    }
  }

  private def client(service: ServiceApi, mode: String = "enforce", failClosed: Boolean = true) =
    new DQClient(spark, DQConfig("http://unused", "silver.transactions", mode, failClosed, 10, "spec_job"), service)

  private val params = Map(
    "partition_start" -> "2026-10-17 00:00:00",
    "partition_end" -> "2026-10-18 00:00:00",
    "source_row_count" -> "7")

  /** TX1 hợp lệ, TX2..TX7 mỗi dòng vi phạm đúng một rule row-level. */
  private lazy val batch: DataFrame = spark.createDataFrame(Seq(
    ("TX1", "SUB1", "84912345678", "10000", "SUCCESS", "HN", "2026-10-17 23:30:00"),
    ("TX2", "SUB2", "12345", "10000", "SUCCESS", "HN", "2026-10-17 01:00:00"),
    ("TX3", null, "84912345679", "10000", "FAILED", "HN", "2026-10-17 02:00:00"),
    ("TX4", "SUB4", "84912345670", "-5", "SUCCESS", "HN", "2026-10-17 03:00:00"),
    ("TX5", "SUB5", "84912345671", "10000", "PENDING", "HN", "2026-10-17 04:00:00"),
    ("TX6", "SUB6", "84912345672", "10000", "SUCCESS", "HN", "2026-10-16 10:00:00"),
    ("TX7", "SUB7", "84912345673", "50k", "SUCCESS", "HN", "2026-10-17 05:00:00")
  )).toDF("transaction_id", "subscriber_id", "msisdn", "amount", "status", "province_code", "event_time")
    .withColumn("amount", col("amount").cast("decimal(18,2)"))
    .withColumn("event_time", to_timestamp(col("event_time")))
    .withColumn("dt", lit("2026-10-17"))

  test("kết quả từng rule, quarantine theo rule id, payload gửi service") {
    val service = new FakeService()
    val res = client(service).validate(batch, "dt=2026-10-17", params)

    val byId = res.results.map(r => r.ruleId -> r).toMap
    byId.keySet should have size 21
    def status(id: String) = byId(id).status

    Seq("row_count_reconcile", "transaction_id_not_null", "msisdn_not_null", "status_not_null", "event_time_not_null",
      "province_code_completeness", "transaction_id_unique", "business_duplicate", "schema_exact", "amount_type",
      "event_time_type", "freshness_end_of_day").foreach(id => withClue(id)(status(id) shouldBe RuleResult.Passed))
    Seq("row_count_min", "subscriber_id_not_null", "amount_not_null", "msisdn_format", "amount_range", "status_domain",
      "msisdn_carrier_prefix", "amount_mean", "event_time_in_partition")
      .foreach(id => withClue(id)(status(id) shouldBe RuleResult.Failed))

    byId("row_count_min").metricValue shouldBe Some(7.0)
    byId("msisdn_format").threshold shouldBe Some(0.97)
    byId("msisdn_format").metricValue.get shouldBe (6.0 / 7 +- 1e-9)
    Seq("subscriber_id_not_null", "amount_not_null", "msisdn_format", "amount_range", "status_domain",
      "event_time_in_partition").foreach(id => withClue(id)(byId(id).failedRows shouldBe Some(1L)))
    byId("transaction_id_not_null").failedRows shouldBe Some(0L)
    byId("msisdn_carrier_prefix").failedRows shouldBe None // không row_level -> không vào quarantine

    res.verdict shouldBe "WARN"
    res.quarantinedRows shouldBe 6
    res.validDf.select("transaction_id").collect().map(_.getString(0)) shouldBe Array("TX1")
    res.validDf.columns should not contain RowLevel.ErrorsColumn
    res.validDf.columns.exists(_.startsWith(DeequChecks.ColumnPrefix)) shouldBe false
    res.quarantineDf.get.select("transaction_id", RowLevel.ErrorsColumn).collect()
      .map(r => r.getString(0) -> r.getSeq[String](1)).toMap shouldBe Map(
      "TX2" -> Seq("msisdn_format"),
      "TX3" -> Seq("subscriber_id_not_null"),
      "TX4" -> Seq("amount_range"),
      "TX5" -> Seq("status_domain"),
      "TX6" -> Seq("event_time_in_partition"),
      "TX7" -> Seq("amount_not_null"))

    val p = service.posted.get
    p.get("client").asText shouldBe "scala"
    p.get("engine").asText shouldBe "deequ-2.0.21-spark-3.5"
    p.get("suite_version").asInt shouldBe 3
    p.get("row_count").asLong shouldBe 7
    p.get("quarantined_rows").asLong shouldBe 6
    p.get("parameters").fieldNames().asScala.toSet shouldBe params.keySet // không lộ tên temp view
    p.get("results").size shouldBe 21
    // view của client tên dq_batch_<run_id>, run_id bắt đầu bằng job name
    spark.catalog.listTables().collect().map(_.name).filter(_.startsWith("dq_batch_spec_job")) shouldBe empty
  }

  test("verdict FAIL: enforce không publish, monitor hạ xuống WARN") {
    client(new FakeService("FAIL")).validate(batch, "dt=2026-10-17", params).shouldPublish shouldBe false
    val monitored = client(new FakeService("FAIL"), mode = "monitor").validate(batch, "dt=2026-10-17", params)
    monitored.verdict shouldBe "WARN"
    monitored.shouldPublish shouldBe true
  }

  test("service lỗi: fail-closed ném DQFailedException, fail-open trả SKIPPED với dữ liệu nguyên vẹn") {
    intercept[DQFailedException](client(new FakeService(getFails = true)).validate(batch, null, params))
      .getMessage should include("connection refused")
    intercept[DQFailedException](client(new FakeService(postFails = true)).validate(batch, null, params))
      .getMessage should include("503")

    val skipped = client(new FakeService(getFails = true), failClosed = false).validate(batch, null, params)
    skipped.verdict shouldBe "SKIPPED"
    skipped.shouldPublish shouldBe true
    skipped.validDf.count() shouldBe 7
    skipped.quarantineDf shouldBe None
  }

  test("fail-open không áp dụng cho 4xx (sai token, chưa có suite): vẫn ném DQFailedException") {
    Seq(401, 404).foreach { status =>
      val e = intercept[DQFailedException](
        client(new FakeService(getFails = true, failStatus = Some(status)), failClosed = false).validate(batch, null, params))
      e.getMessage should include(s"HTTP $status")
    }
    client(new FakeService(getFails = true, failStatus = Some(503)), failClosed = false)
      .validate(batch, null, params).verdict shouldBe "SKIPPED"
  }

  test("cache: client chỉ gỡ cache do mình tạo; gỡ khi lỗi và khi gọi release()") {
    import org.apache.spark.storage.StorageLevel
    val fresh = batch.where("amount IS NULL OR amount IS NOT NULL") // plan riêng, chưa cache
    fresh.storageLevel shouldBe StorageLevel.NONE

    val res = client(new FakeService()).validate(fresh, null, params)
    fresh.storageLevel should not be StorageLevel.NONE
    res.release()
    fresh.storageLevel shouldBe StorageLevel.NONE

    intercept[IllegalArgumentException](client(new FakeService()).validate(fresh, null, Map.empty[String, String]))
    fresh.storageLevel shouldBe StorageLevel.NONE

    val jobCached = batch.where("transaction_id IS NOT NULL").cache() // job tự cache -> client không gỡ
    client(new FakeService()).validate(jobCached, null, params).release()
    jobCached.storageLevel should not be StorageLevel.NONE
    jobCached.unpersist()
  }

  test("job thiếu suite parameter -> lỗi rõ ràng, không gửi run") {
    val service = new FakeService()
    intercept[IllegalArgumentException](client(service).validate(batch, null, params - "source_row_count"))
      .getMessage should include("source_row_count")
    service.posted shouldBe None
  }

  test("validateForPython: nhận java.util.Map, ghi client = python") {
    val service = new FakeService()
    val res = client(service).validateForPython(batch, "dt=2026-10-17", params.asJava)
    service.posted.get.get("client").asText shouldBe "python"
    res.reasonsJava.asScala shouldBe Seq("[warn][completeness] r")
    res.suiteVersionOrNull shouldBe 3
  }
}
