package dqplatform.client

import com.amazon.deequ.analyzers.{AnalyzerOptions, FilteredRowOutcome}
import com.amazon.deequ.checks.{Check, CheckLevel}
import dqplatform.client.CheckSpec._

/**
 * Rule spec v2 -> Deequ Check (docs/DEEQU_MIGRATION_PLAN.md bảng 5.2).
 * Mỗi rule là một Check riêng: kết quả theo từng rule, và cột row-level mang tên rule.
 */
object DeequChecks {

  /** Tiền tố tên cột row-level (= check.description) để không trùng cột dữ liệu. */
  val ColumnPrefix = "_dq_rule_"

  def columnName(ruleId: String): String = ColumnPrefix + ruleId

  def isDeequRule(rule: Rule): Boolean = rule.check match {
    case _: SchemaColumns | _: SchemaType => false
    case _                                => true
  }

  /** params phải chứa cả Params.Batch (tên temp view) nếu suite có sql_metric. */
  def build(rule: Rule, params: Map[String, String]): Check = {
    val level = if (rule.severity == "error") CheckLevel.Error else CheckLevel.Warning
    val base = Check(level, columnName(rule.id))
    def fn(a: Assertion) = Params.resolve(a, params).fn
    def fnOrAllRows(a: Option[Assertion]) = a.map(fn).getOrElse(ResolvedAssertion.AllRows.fn)

    rule.check match {
      case Size(a) =>
        val f = fn(a)
        base.hasSize(n => f(n.toDouble))
      case Complete(column)        => base.isComplete(column)
      case Completeness(column, a) => base.hasCompleteness(column, fn(a))
      case Unique(columns, a)      => base.hasUniqueness(columns, fnOrAllRows(a))
      case Pattern(column, regex, a) =>
        // Deequ coi null là không khớp; GX/rule spec bỏ qua null (null do rule completeness lo):
        // where -> mẫu số chỉ gồm dòng không null, FilteredRowOutcome.TRUE -> dòng null đạt ở row-level.
        base
          .hasPattern(column, regex.r, fnOrAllRows(a),
            analyzerOptions = Some(AnalyzerOptions(filteredRow = FilteredRowOutcome.TRUE)))
          .where(s"`$column` IS NOT NULL")
      case Range(column, min, max, includeMin, includeMax) =>
        base.isContainedIn(column, min, max, includeLowerBound = includeMin, includeUpperBound = includeMax)
      case InSet(column, values) => base.isContainedIn(column, values.toArray)
      case Mean(column, a)       => base.hasMean(column, fn(a))
      case SqlPredicate(predicate, columns, a) =>
        base.satisfies(Params.substitute(predicate, params), rule.id, fnOrAllRows(a), columns = columns.toList)
      case SqlMetric(sql, a) => base.customSql(Params.substitute(sql, params), fn(a))
      case _: SchemaColumns | _: SchemaType =>
        throw new IllegalArgumentException(s"rule ${rule.id}: kind ${rule.check.kind} chạy ở SchemaRules, không qua Deequ")
    }
  }

  /** Ngưỡng gửi lên service: rule tỉ lệ không ghi assert = 1.0 (mọi dòng phải đạt). */
  def threshold(rule: Rule, params: Map[String, String]): Option[Double] = {
    def of(a: Option[Assertion]) = a.map(Params.resolve(_, params).threshold).getOrElse(Some(1.0))
    rule.check match {
      case _: Complete | _: Range | _: InSet => Some(1.0)
      case Completeness(_, a)                => of(Some(a))
      case Unique(_, a)                      => of(a)
      case Pattern(_, _, a)                  => of(a)
      case SqlPredicate(_, _, a)             => of(a)
      case _                                 => None
    }
  }
}
