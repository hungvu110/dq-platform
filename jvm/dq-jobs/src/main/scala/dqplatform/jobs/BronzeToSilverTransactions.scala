package dqplatform.jobs

import dqplatform.client.{DQClient, DQFailedException}
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

import java.time.LocalDate

/**
 * Bronze -> Silver cho transactions (bản Scala của spark/jobs/bronze_to_silver.py), DQ gate theo
 * Write-Audit-Publish: transform -> AUDIT (DQClient) -> WRITE staging -> PUBLISH (rename vào partition).
 *
 * Ghi vào thư mục riêng để chạy song song với job Python:
 *   <lake>/<target-layer>/transactions/dt=...   (mặc định target-layer = silver_scala)
 *   <spark.dq.quarantine.path>/transactions_scala, <spark.dq.staging.path>/transactions_scala/<run_id>
 *
 * Exit code (hợp đồng với Airflow): 0 = đã publish, 2 = DQ service lỗi (fail-closed), 3 = verdict FAIL.
 */
object BronzeToSilverTransactions {

  val AppName = "bronze_to_silver_transactions_scala"
  val ExitPublished = 0
  val ExitServiceDown = 2
  val ExitDqFailed = 3

  private val log = LoggerFactory.getLogger(getClass)

  final case class Args(dt: LocalDate, lake: String = "/data/lake", targetLayer: String = "silver_scala")

  object Args {
    def parse(argv: Seq[String]): Args = {
      val kv = argv.grouped(2).collect { case Seq(k, v) if k.startsWith("--") => k.drop(2) -> v }.toMap
      val unknown = kv.keySet -- Set("dt", "lake", "target-layer")
      require(argv.size % 2 == 0 && unknown.isEmpty, s"tham số không hợp lệ: ${argv.mkString(" ")}")
      val dt = kv.getOrElse("dt", throw new IllegalArgumentException("thiếu --dt yyyy-MM-dd"))
      Args(LocalDate.parse(dt), kv.getOrElse("lake", "/data/lake"), kv.getOrElse("target-layer", "silver_scala"))
    }
  }

  def main(argv: Array[String]): Unit = {
    val args = Args.parse(argv.toSeq)
    val spark = SparkSession.builder().appName(AppName).getOrCreate()
    val code =
      try run(spark, args, DQClient.fromSession(spark))
      finally spark.stop()
    sys.exit(code)
  }

  def run(spark: SparkSession, args: Args, dq: DQClient): Int = {
    val dt = args.dt.toString
    val conf = spark.conf
    val silver = s"${args.lake}/${args.targetLayer}/transactions/dt=$dt"
    val quarantineRoot = conf.get("spark.dq.quarantine.path", s"${args.lake}/quarantine") + "/transactions_scala"
    val stagingRoot = conf.get("spark.dq.staging.path", s"${args.lake}/_staging") + "/transactions_scala"

    // 1. Transform
    val raw = spark.read.parquet(s"${args.lake}/bronze/transactions/dt=$dt")
    val df = TransactionsTransform.transform(raw, dt)

    // 2. Audit: dữ kiện runtime cho suite; ngưỡng nằm trong suite, không nằm ở đây
    val params = Map(
      "partition_start" -> s"$dt 00:00:00",
      "partition_end" -> s"${args.dt.plusDays(1)} 00:00:00",
      "source_row_count" -> TransactionsTransform.sourceRowCount(raw).toString)
    val res =
      try dq.validate(df, s"dt=$dt", params)
      catch {
        case e: DQFailedException =>
          log.error(s"DQ ERROR: ${e.getMessage}")
          return ExitServiceDown
      }

    try {
      Wap.writeQuarantine(res.quarantineDf, res.runId, quarantineRoot, dt, spark.sparkContext.hadoopConfiguration)
      if (!res.shouldPublish) {
        log.error(s"DQ ${res.verdict}: không publish $silver. Lý do: ${res.reasons.mkString("; ")}")
        return ExitDqFailed // Airflow task fail -> chặn downstream
      }

      // 3. Write staging -> 4. Publish
      Wap.publish(res.validDf.drop("dt"), s"$stagingRoot/${res.runId}", silver)
      log.info(s"DQ ${res.verdict}: published ${res.rowCount - res.quarantinedRows} rows -> $silver " +
        s"(quarantined ${res.quarantinedRows}, run ${res.runId})")
      ExitPublished
    } finally res.release()
  }
}
