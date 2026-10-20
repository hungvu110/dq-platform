package dqplatform.client

import com.amazon.deequ.checks.CheckResult
import com.amazon.deequ.constraints.ConstraintStatus
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode

/** Kết quả một rule, format trung lập gửi lên dq-service (không phụ thuộc Deequ). */
final case class RuleResult(
    ruleId: String,
    kind: String,
    column: Option[String],
    dimension: String,
    severity: String,
    rowLevel: Boolean,
    threshold: Option[Double],
    status: String,
    metricName: Option[String],
    metricValue: Option[Double],
    failedRows: Option[Long],
    message: Option[String]) {

  def passed: Boolean = status == RuleResult.Passed

  def toJson(mapper: ObjectMapper): ObjectNode = {
    val n = mapper.createObjectNode()
    n.put("rule_id", ruleId)
    n.put("kind", kind)
    column.fold(n.putNull("column"))(n.put("column", _))
    n.put("dimension", dimension)
    n.put("severity", severity)
    n.put("row_level", rowLevel)
    threshold.fold(n.putNull("threshold"))(v => n.put("threshold", v))
    n.put("status", status)
    metricName.fold(n.putNull("metric_name"))(n.put("metric_name", _))
    metricValue.filterNot(v => v.isNaN || v.isInfinite).fold(n.putNull("metric_value"))(v => n.put("metric_value", v))
    failedRows.fold(n.putNull("failed_rows"))(v => n.put("failed_rows", v))
    message.fold(n.putNull("message"))(n.put("message", _))
    n
  }
}

object RuleResult {
  val Passed = "passed"
  val Failed = "failed"
  /** Rule không chạy được (cột không tồn tại, SQL lỗi...). Service coi như fail theo severity. */
  val Error = "error"

  def of(rule: Rule, threshold: Option[Double], status: String, metricName: Option[String],
         metricValue: Option[Double], message: Option[String]): RuleResult =
    RuleResult(rule.id, rule.check.kind, rule.check.target, rule.dimension, rule.severity, rule.rowLevel,
      threshold, status, metricName, metricValue, None, message)

  /** Mỗi Check có đúng một constraint (DeequChecks dựng mỗi rule một Check). */
  def fromDeequ(rule: Rule, threshold: Option[Double], checkResult: CheckResult): RuleResult = {
    val cr = checkResult.constraintResults.head
    val metric = cr.metric
    val value = metric.flatMap(_.value.toOption).collect { case d: Double => d }
    val analyzerFailed = metric.isEmpty || metric.exists(_.value.isFailure)
    val status = cr.status match {
      case ConstraintStatus.Success                  => Passed
      case ConstraintStatus.Failure if analyzerFailed => Error
      case _                                         => Failed
    }
    of(rule, threshold, status, metric.map(_.name), value, cr.message)
  }
}
