package dqplatform.client

import scala.util.matching.Regex

/** Assertion đã thay parameter -> hàm kiểm metric + ngưỡng gửi lên service. */
final case class ResolvedAssertion(op: String, value: Option[Double], min: Option[Double], max: Option[Double]) {

  def fn: Double => Boolean = op match {
    case ">="      => v => v >= value.get
    case ">"       => v => v > value.get
    case "<="      => v => v <= value.get
    case "<"       => v => v < value.get
    case "=="      => v => v == value.get
    case "between" => v => v >= min.get && v <= max.get
  }

  /** Ngưỡng dưới của rule dạng tỉ lệ (vd mostly 0.97). Service dùng để biết rule row-level có "chịu lỗi" không. */
  def threshold: Option[Double] = op match {
    case ">=" | ">" | "==" => value
    case "between"         => min
    case _                 => None
  }
}

object ResolvedAssertion {
  /** Mặc định của rule dạng tỉ lệ khi suite không ghi assert: mọi dòng phải đạt. */
  val AllRows: ResolvedAssertion = ResolvedAssertion("==", Some(1.0), None, None)
}

object Params {
  /** Tên parameter dành riêng: temp view chứa batch đang validate (dùng trong sql_metric). */
  val Batch = "batch"

  private val Placeholder = """\$\{([A-Za-z_][A-Za-z0-9_]*)\}""".r
  /** Giá trị được ghép thẳng vào SQL -> chỉ cho ký tự của ngày giờ / số / tên. Chặn cả ' lẫn \ (escape trong literal). */
  private val SafeValue = "[A-Za-z0-9_:.+ -]*" // dùng với String.matches (khớp toàn chuỗi, kể cả \n cuối)

  /** Job phải truyền đủ parameter suite khai báo, giá trị chỉ gồm ký tự an toàn. */
  def requireAll(declared: Seq[String], values: Map[String, String]): Unit = {
    val missing = declared.filterNot(values.contains)
    if (missing.nonEmpty) throw new IllegalArgumentException(s"job chưa truyền suite parameter: ${missing.mkString(", ")}")
    values.collectFirst { case (k, v) if !v.matches(SafeValue) => k }.foreach { k =>
      throw new IllegalArgumentException(
        s"suite parameter '$k' chứa ký tự không cho phép (chỉ nhận chữ, số, khoảng trắng và _ : . + -)")
    }
  }

  def substitute(template: String, values: Map[String, String]): String =
    Placeholder.replaceAllIn(template, m => values.get(m.group(1)) match {
      case Some(v) => Regex.quoteReplacement(v)
      case None    => throw new IllegalArgumentException(s"thiếu suite parameter '${m.group(1)}' trong: $template")
    })

  def resolve(bound: Bound, values: Map[String, String]): Double = bound match {
    case Bound.Literal(v) => v
    case Bound.Param(name) =>
      val raw = values.getOrElse(name, throw new IllegalArgumentException(s"thiếu suite parameter '$name'"))
      try raw.toDouble
      catch { case _: NumberFormatException => throw new IllegalArgumentException(s"suite parameter '$name'='$raw' không phải số") }
  }

  def resolve(a: Assertion, values: Map[String, String]): ResolvedAssertion =
    ResolvedAssertion(a.op, a.value.map(resolve(_, values)), a.min.map(resolve(_, values)), a.max.map(resolve(_, values)))
}
