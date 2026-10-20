package dqplatform.client

import dqplatform.client.CheckSpec.{SchemaColumns, SchemaType}
import org.apache.spark.sql.types._

/**
 * Rule schema chạy trên df.schema, không cần Spark job (D5 trong plan): Deequ không có so khớp
 * chính xác tập cột, và hasDataType chỉ suy kiểu cho cột string.
 */
object SchemaRules {

  def evaluate(rule: Rule, schema: StructType): RuleResult = rule.check match {
    case SchemaColumns(expected, exact) =>
      val actual = schema.fieldNames.toSeq
      val missing = expected.filterNot(actual.contains)
      val extra = if (exact) actual.filterNot(expected.contains) else Nil
      val ok = missing.isEmpty && extra.isEmpty
      val msg = Seq(
        if (missing.nonEmpty) Some(s"thiếu cột: ${missing.mkString(", ")}") else None,
        if (extra.nonEmpty) Some(s"thừa cột: ${extra.mkString(", ")}") else None).flatten.mkString("; ")
      result(rule, ok, "SchemaColumns", Option(msg).filter(_.nonEmpty))

    case SchemaType(column, expected) =>
      schema.find(_.name == column) match {
        case None => RuleResult.of(rule, None, RuleResult.Error, Some("SchemaType"), None, Some(s"không có cột $column"))
        case Some(field) =>
          val ok = matches(field.dataType, expected)
          result(rule, ok, "SchemaType",
            if (ok) None else Some(s"cột $column có kiểu ${field.dataType.simpleString}, cần $expected"))
      }

    case other => throw new IllegalArgumentException(s"rule ${rule.id}: ${other.kind} không phải rule schema")
  }

  private def result(rule: Rule, ok: Boolean, metric: String, message: Option[String]): RuleResult =
    RuleResult.of(rule, None, if (ok) RuleResult.Passed else RuleResult.Failed, Some(metric),
      Some(if (ok) 1.0 else 0.0), message)

  private def matches(actual: DataType, expected: String): Boolean = (actual, expected) match {
    case (StringType, "string")       => true
    case (BooleanType, "boolean")     => true
    case (IntegerType, "int")         => true
    case (LongType, "long")           => true
    case (FloatType, "float")         => true
    case (DoubleType, "double")       => true
    case (_: DecimalType, "decimal")  => true
    case (DateType, "date")           => true
    case (TimestampType, "timestamp") => true
    case _                            => false
  }
}
