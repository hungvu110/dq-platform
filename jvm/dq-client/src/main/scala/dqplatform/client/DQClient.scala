package dqplatform.client

import com.amazon.deequ.VerificationSuite
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel
import org.slf4j.LoggerFactory

import java.time.Duration
import java.util.{Properties, UUID}
import scala.collection.JavaConverters._
import scala.util.control.NonFatal

/** Batch không được kiểm DQ vì service lỗi, ở chế độ fail-closed. Job nên exit 2. */
final class DQFailedException(message: String, cause: Throwable) extends RuntimeException(message, cause)

final case class DQResult(
    runId: String,
    verdict: String, // PASS | WARN | FAIL | SKIPPED
    reasons: Seq[String],
    rowCount: Long,
    quarantinedRows: Long,
    validDf: DataFrame,
    quarantineDf: Option[DataFrame],
    suiteVersion: Option[Int],
    results: Seq[RuleResult])(releaseFn: () => Unit = () => ()) {

  def shouldPublish: Boolean = Set("PASS", "WARN", "SKIPPED").contains(verdict)

  /** Gỡ cache client tạo cho batch. Gọi sau khi đã ghi xong validDf / quarantineDf. */
  def release(): Unit = releaseFn()

  // Accessor kiểu Java cho wrapper Python (py4j không hiểu Option / Seq của Scala)
  def reasonsJava: java.util.List[String] = reasons.asJava
  def quarantineDfOrNull: DataFrame = quarantineDf.orNull
  def suiteVersionOrNull: Integer = suiteVersion.map(Int.box).orNull
}

/**
 * Cấu hình đọc từ spark.conf (giống bản Python trước đây):
 *   spark.dq.service.url, spark.dq.dataset (bắt buộc), spark.dq.mode (enforce | monitor),
 *   spark.dq.fail.on.service.error (mặc định true), spark.dq.service.timeout.sec (mặc định 10).
 * Token: biến môi trường DQ_API_TOKEN (không đặt trong spark.conf vì sẽ hiện trên Spark UI).
 */
final case class DQConfig(
    serviceUrl: String,
    dataset: String,
    mode: String,
    failOnServiceError: Boolean,
    timeoutSec: Int,
    jobName: String)

object DQConfig {
  def fromSpark(spark: SparkSession): DQConfig = {
    val c = spark.conf
    def required(key: String) =
      c.getOption(key).map(_.trim).filter(_.nonEmpty).getOrElse(throw new IllegalArgumentException(s"thiếu spark conf $key"))
    val mode = c.get("spark.dq.mode", "enforce")
    require(mode == "enforce" || mode == "monitor", s"spark.dq.mode='$mode' phải là enforce | monitor")
    DQConfig(
      serviceUrl = required("spark.dq.service.url"),
      dataset = required("spark.dq.dataset"),
      mode = mode,
      failOnServiceError = c.get("spark.dq.fail.on.service.error", "true").toBoolean,
      timeoutSec = c.get("spark.dq.service.timeout.sec", "10").toInt,
      jobName = c.get("spark.app.name", "unknown"))
  }
}

/**
 * Thư viện nhúng vào mọi job Spark: lấy suite (HTTP) -> chạy rule bằng Deequ trong Spark ->
 * tách quarantine -> gửi kết quả (HTTP) -> nhận verdict từ service.
 * Dữ liệu không rời cluster: chỉ suite và kết quả (vài KB) đi qua mạng, và chỉ driver gọi HTTP.
 */
final class DQClient(spark: SparkSession, val config: DQConfig, service: ServiceApi, clientName: String = "scala") {

  private val log = LoggerFactory.getLogger(classOf[DQClient])
  private val mapper = new ObjectMapper()

  /** params: dữ kiện runtime mà suite tham chiếu (${x} / {"param": x}); ngưỡng nằm trong suite. */
  def validate(df: DataFrame, partitionSpec: String, params: Map[String, String]): DQResult = {
    val runId = s"${config.jobName}-${UUID.randomUUID().toString.replace("-", "").take(12)}"
    // chỉ gỡ cache do chính client tạo; DataFrame job đã cache sẵn thì để job quản lý
    val ownsCache = df.storageLevel == StorageLevel.NONE
    val data = if (ownsCache) df.cache() else df
    val release: () => Unit = () => if (ownsCache) data.unpersist()
    try run(runId, data, partitionSpec, params, release)
    catch {
      case NonFatal(e) =>
        release()
        throw e
    }
  }

  private def run(runId: String, data: DataFrame, partitionSpec: String, params: Map[String, String],
                  release: () => Unit): DQResult = {
    val rowCount = data.count()

    val suiteResponse =
      try service.getSuite(config.dataset)
      catch { case NonFatal(e) => return serviceDown(e, runId, data, rowCount, release) }
    val suite = RuleSpec.parseSuite(suiteResponse.get("suite"))
    val suiteVersion = Option(suiteResponse.get("version")).filter(_.canConvertToInt).map(_.asInt)
    Params.requireAll(suite.parameters, params)

    val view = s"dq_batch_${runId.replaceAll("[^A-Za-z0-9_]", "_")}"
    data.createOrReplaceTempView(view)
    val (results, split) =
      try evaluate(suite, data, params + (Params.Batch -> view))
      // không dùng spark.catalog.dropTempView: hàm đó gỡ luôn cache của plan sau view (= batch đang cache)
      finally spark.sessionState.catalog.dropTempView(view)

    val payload = buildPayload(runId, partitionSpec, suiteVersion, params, rowCount, split.quarantinedRows, results)
    val decision =
      try service.postRun(payload)
      catch { case NonFatal(e) => return serviceDown(e, runId, data, rowCount, release) }

    val reasons = decision.get("reasons").elements().asScala.map(_.asText()).toList
    val verdict = decision.get("verdict").asText() match {
      case "FAIL" if config.mode == "monitor" =>
        log.warn("verdict FAIL nhưng spark.dq.mode=monitor -> vẫn publish")
        "WARN"
      case v => v
    }
    log.info(s"run=$runId dataset=${config.dataset} rows=$rowCount quarantined=${split.quarantinedRows} " +
      s"verdict=$verdict reasons=${reasons.mkString("[", "; ", "]")}")
    Option(decision.get("dimensions")).foreach(_.fields().asScala.foreach { e =>
      log.info(f"  ${e.getKey}%-12s ${e.getValue.get("passed").asInt}/${e.getValue.get("evaluated").asInt} rule pass")
    })

    DQResult(runId, verdict, reasons, rowCount, split.quarantinedRows, split.valid, split.quarantine, suiteVersion,
      results)(release)
  }

  /** Cho wrapper Python (py4j): Map của Java, ghi nhận client = python. */
  def validateForPython(df: DataFrame, partitionSpec: String, params: java.util.Map[String, String]): DQResult =
    new DQClient(spark, config, service, "python").validate(df, partitionSpec, params.asScala.toMap)

  /** Rule schema chạy trên df.schema, còn lại chạy chung một VerificationSuite (Deequ gộp analyzer). */
  private def evaluate(suite: Suite, data: DataFrame, values: Map[String, String]): (Seq[RuleResult], RowLevel.Split) = {
    val (deequRules, schemaRules) = suite.rules.partition(DeequChecks.isDeequRule)
    val schemaResults = schemaRules.map(SchemaRules.evaluate(_, data.schema))
    val checks = deequRules.map(r => r -> DeequChecks.build(r, values))
    val verification = VerificationSuite().onData(data).addChecks(checks.map(_._2)).run()
    val split = RowLevel.split(spark, verification, data, deequRules.filter(_.rowLevel).map(_.id))
    val deequResults = checks.map { case (rule, check) =>
      RuleResult.fromDeequ(rule, DeequChecks.threshold(rule, values), verification.checkResults(check))
        .copy(failedRows = split.failedRowsByRule.get(rule.id))
    }
    val byId = (schemaResults ++ deequResults).map(r => r.ruleId -> r).toMap
    (suite.rules.map(r => byId(r.id)), split) // giữ thứ tự như trong suite
  }

  private def buildPayload(runId: String, partitionSpec: String, suiteVersion: Option[Int], params: Map[String, String],
                           rowCount: Long, quarantinedRows: Long, results: Seq[RuleResult]): ObjectNode = {
    val p = mapper.createObjectNode()
    p.put("run_id", runId)
    p.put("dataset", config.dataset)
    Option(partitionSpec).fold(p.putNull("partition_spec"))(p.put("partition_spec", _))
    p.put("job_name", config.jobName)
    p.put("client", clientName)
    p.put("engine", DQClient.Engine)
    suiteVersion.fold(p.putNull("suite_version"))(v => p.put("suite_version", v))
    val ps = p.putObject("parameters")
    params.toSeq.sortBy(_._1).foreach { case (k, v) => ps.put(k, v) }
    p.put("row_count", rowCount)
    p.put("quarantined_rows", quarantinedRows)
    val rs = p.putArray("results")
    results.foreach(r => rs.add(r.toJson(mapper)))
    p
  }

  private def serviceDown(err: Throwable, runId: String, data: DataFrame, rowCount: Long,
                          release: () => Unit): DQResult = err match {
    // 4xx = sai token / chưa có suite / payload bị từ chối: lỗi cấu hình, không bao giờ fail-open
    case e: ServiceException if e.isClientError =>
      throw new DQFailedException(s"DQ service từ chối request (${e.getMessage}); không fail-open với lỗi 4xx", e)
    case _ if config.failOnServiceError =>
      throw new DQFailedException(s"DQ service unavailable (${err.getMessage}); spark.dq.fail.on.service.error=true", err)
    case _ =>
      log.error(s"DQ service unavailable (${err.getMessage}) -> SKIPPED (fail-open)")
      DQResult(runId, "SKIPPED", Seq(String.valueOf(err.getMessage)), rowCount, 0L, data, None, None, Nil)(release)
  }
}

object DQClient {

  /** vd deequ-2.0.21-spark-3.5, lấy từ dq-client.properties (Maven điền lúc build). */
  lazy val Engine: String = {
    val props = new Properties()
    Option(getClass.getResourceAsStream("/dq-client.properties")).foreach { in =>
      try props.load(in) finally in.close()
    }
    s"deequ-${props.getProperty("deequ.version", "unknown")}"
  }

  def fromSession(spark: SparkSession): DQClient = {
    val config = DQConfig.fromSpark(spark)
    val token = sys.env.getOrElse("DQ_API_TOKEN", "")
    new DQClient(spark, config, new HttpServiceApi(config.serviceUrl, token, Duration.ofSeconds(config.timeoutSec.toLong)))
  }
}
