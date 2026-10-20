package dqplatform.client

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.JavaConverters._
import scala.util.Try

/** Suite lỗi định dạng (service đã kiểm khi PUT; client vẫn kiểm để báo lỗi rõ ràng). */
final class SuiteFormatException(message: String) extends IllegalArgumentException(message)

/** Giá trị trong assertion: số cố định hoặc tham chiếu suite parameter {"param": "..."}. */
sealed trait Bound
object Bound {
  final case class Literal(value: Double) extends Bound
  final case class Param(name: String) extends Bound
}

/** op: >= > <= < == between. between dùng min/max, còn lại dùng value. */
final case class Assertion(op: String, value: Option[Bound], min: Option[Bound], max: Option[Bound])

sealed trait CheckSpec {
  def kind: String
  /** Cột (hoặc danh sách cột) mà rule nhắm tới, để báo cáo. */
  def target: Option[String] = None
}

object CheckSpec {
  final case class Size(assertion: Assertion) extends CheckSpec { val kind = "size" }
  final case class Complete(column: String) extends CheckSpec {
    val kind = "complete"; override def target: Option[String] = Some(column)
  }
  final case class Completeness(column: String, assertion: Assertion) extends CheckSpec {
    val kind = "completeness"; override def target: Option[String] = Some(column)
  }
  final case class Unique(columns: Seq[String], assertion: Option[Assertion]) extends CheckSpec {
    val kind = "unique"; override def target: Option[String] = Some(columns.mkString(","))
  }
  final case class Pattern(column: String, regex: String, assertion: Option[Assertion]) extends CheckSpec {
    val kind = "pattern"; override def target: Option[String] = Some(column)
  }
  final case class Range(column: String, min: Double, max: Double, includeMin: Boolean, includeMax: Boolean)
    extends CheckSpec { val kind = "range"; override def target: Option[String] = Some(column) }
  final case class InSet(column: String, values: Seq[String]) extends CheckSpec {
    val kind = "in_set"; override def target: Option[String] = Some(column)
  }
  final case class Mean(column: String, assertion: Assertion) extends CheckSpec {
    val kind = "mean"; override def target: Option[String] = Some(column)
  }
  final case class SqlPredicate(predicate: String, columns: Seq[String], assertion: Option[Assertion])
    extends CheckSpec { val kind = "sql_predicate"; override def target: Option[String] = Some(columns.mkString(",")) }
  final case class SqlMetric(sql: String, assertion: Assertion) extends CheckSpec { val kind = "sql_metric" }
  final case class SchemaColumns(columns: Seq[String], exact: Boolean) extends CheckSpec { val kind = "schema_columns" }
  final case class SchemaType(column: String, dataType: String) extends CheckSpec {
    val kind = "schema_type"; override def target: Option[String] = Some(column)
  }

  /** Kind có kết quả theo dòng -> được phép row_level=true. */
  val RowLevelKinds: Set[String] = Set("complete", "completeness", "unique", "pattern", "range", "in_set", "sql_predicate")
  val SchemaTypes: Set[String] = Set("string", "boolean", "int", "long", "float", "double", "decimal", "date", "timestamp")
}

final case class Rule(
    id: String,
    dimension: String,
    severity: String,
    rowLevel: Boolean,
    description: Option[String],
    check: CheckSpec)

final case class Suite(parameters: Seq[String], rules: Seq[Rule])

object RuleSpec {
  val Format = "dq-rules/v2"
  val Dimensions: Set[String] = Set("completeness", "uniqueness", "validity", "accuracy", "consistency", "timeliness")
  private val IdPattern = "^[a-z][a-z0-9_]*$".r
  private val mapper = new ObjectMapper()

  def parseSuite(json: String): Suite = parseSuite(mapper.readTree(json))

  def parseSuite(node: JsonNode): Suite = {
    val format = str(node, "format", "suite")
    if (format != Format) fail(s"suite.format='$format', chỉ hỗ trợ '$Format'")
    val params = Option(node.get("parameters")).map(_.fieldNames().asScala.toList).getOrElse(Nil)
    val rules = arr(node, "rules", "suite").zipWithIndex.map { case (n, i) => parseRule(n, s"rules[$i]") }
    if (rules.isEmpty) fail("suite.rules rỗng")
    val dup = rules.groupBy(_.id).collect { case (id, rs) if rs.size > 1 => id }
    if (dup.nonEmpty) fail(s"rule id bị trùng: ${dup.mkString(", ")}")
    Suite(params, rules)
  }

  private def parseRule(n: JsonNode, path: String): Rule = {
    val id = str(n, "id", path)
    if (IdPattern.findFirstIn(id).isEmpty) fail(s"$path.id='$id' phải khớp ${IdPattern.regex}")
    val dimension = str(n, "dimension", path)
    if (!Dimensions(dimension)) fail(s"$path.dimension='$dimension' phải thuộc ${Dimensions.mkString(", ")}")
    val severity = str(n, "severity", path)
    if (severity != "error" && severity != "warn") fail(s"$path.severity='$severity' phải là error | warn")
    val rowLevel = Option(n.get("row_level")).exists(_.asBoolean(false))
    val check = parseCheck(obj(n, "check", path), s"$path.check")
    if (rowLevel && !CheckSpec.RowLevelKinds(check.kind)) fail(s"$path: kind '${check.kind}' không hỗ trợ row_level")
    Rule(id, dimension, severity, rowLevel, optStr(n, "description"), check)
  }

  private def parseCheck(n: JsonNode, path: String): CheckSpec = {
    import CheckSpec._
    def a = parseAssertion(obj(n, "assert", path), s"$path.assert")
    def optA = Option(n.get("assert")).filterNot(_.isNull).map(parseAssertion(_, s"$path.assert"))
    def col = str(n, "column", path)
    str(n, "kind", path) match {
      case "size"           => Size(a)
      case "complete"       => Complete(col)
      case "completeness"   => Completeness(col, a)
      case "unique"         => Unique(strs(n, "columns", path), optA)
      case "pattern"        =>
        val regex = str(n, "regex", path)
        Try(regex.r).failed.foreach(e => fail(s"$path.regex không hợp lệ: ${e.getMessage}"))
        Pattern(col, regex, optA)
      case "range"          =>
        Range(col, num(n, "min", path), num(n, "max", path),
          Option(n.get("include_min")).forall(_.asBoolean(true)), Option(n.get("include_max")).forall(_.asBoolean(true)))
      case "in_set"         => InSet(col, strs(n, "values", path))
      case "mean"           => Mean(col, a)
      case "sql_predicate"  => SqlPredicate(str(n, "predicate", path), strs(n, "columns", path), optA)
      case "sql_metric"     => SqlMetric(str(n, "sql", path), a)
      case "schema_columns" => SchemaColumns(strs(n, "columns", path), Option(n.get("exact")).exists(_.asBoolean(false)))
      case "schema_type"    =>
        val t = str(n, "type", path)
        if (!SchemaTypes(t)) fail(s"$path.type='$t' phải thuộc ${SchemaTypes.mkString(", ")}")
        SchemaType(col, t)
      case other => fail(s"$path.kind='$other' không được hỗ trợ")
    }
  }

  private def parseAssertion(n: JsonNode, path: String): Assertion = {
    val op = str(n, "op", path)
    def bound(field: String): Option[Bound] = Option(n.get(field)).filterNot(_.isNull).map {
      case v if v.isNumber => Bound.Literal(v.asDouble())
      case v if v.isObject && v.has("param") => Bound.Param(v.get("param").asText())
      case v => fail(s"""$path.$field phải là số hoặc {"param": ...}, nhận $v""")
    }
    val assertion = Assertion(op, bound("value"), bound("min"), bound("max"))
    op match {
      case ">=" | ">" | "<=" | "<" | "==" => if (assertion.value.isEmpty) fail(s"$path: op '$op' cần 'value'")
      case "between" => if (assertion.min.isEmpty || assertion.max.isEmpty) fail(s"$path: op 'between' cần 'min' và 'max'")
      case other => fail(s"$path.op='$other' không được hỗ trợ")
    }
    assertion
  }

  private def fail(msg: String): Nothing = throw new SuiteFormatException(msg)

  private def obj(n: JsonNode, field: String, path: String): JsonNode =
    Option(n.get(field)).filter(_.isObject).getOrElse(fail(s"$path.$field bắt buộc (object)"))

  private def arr(n: JsonNode, field: String, path: String): List[JsonNode] =
    Option(n.get(field)).filter(_.isArray).map(_.elements().asScala.toList).getOrElse(fail(s"$path.$field bắt buộc (array)"))

  private def str(n: JsonNode, field: String, path: String): String =
    Option(n.get(field)).filter(_.isTextual).map(_.asText()).getOrElse(fail(s"$path.$field bắt buộc (string)"))

  private def optStr(n: JsonNode, field: String): Option[String] =
    Option(n.get(field)).filter(_.isTextual).map(_.asText())

  private def strs(n: JsonNode, field: String, path: String): Seq[String] = {
    val values = arr(n, field, path)
    if (values.isEmpty || !values.forall(_.isTextual)) fail(s"$path.$field phải là mảng string không rỗng")
    values.map(_.asText())
  }

  private def num(n: JsonNode, field: String, path: String): Double =
    Option(n.get(field)).filter(_.isNumber).map(_.asDouble()).getOrElse(fail(s"$path.$field bắt buộc (number)"))
}
