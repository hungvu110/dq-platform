package dqplatform.client

import com.amazon.deequ.VerificationResult
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Tách quarantine từ kết quả row-level của Deequ (D3 trong plan): mỗi rule row_level có một cột
 * boolean, dòng có ít nhất một rule fail -> quarantine, cột _dq_errors = danh sách rule id fail.
 */
object RowLevel {

  val ErrorsColumn = "_dq_errors"

  final case class Split(
      valid: DataFrame,
      quarantine: Option[DataFrame],
      quarantinedRows: Long,
      failedRowsByRule: Map[String, Long])

  def split(spark: SparkSession, verification: VerificationResult, data: DataFrame, rowLevelRuleIds: Seq[String]): Split = {
    if (rowLevelRuleIds.isEmpty) return Split(data, None, 0L, Map.empty)

    val withRowLevel = VerificationResult.rowLevelResultsAsDataFrame(spark, verification, data)
    val missing = rowLevelRuleIds.map(DeequChecks.columnName).filterNot(withRowLevel.columns.contains)
    require(missing.isEmpty, s"Deequ không trả kết quả row-level cho: ${missing.mkString(", ")}")

    // null (vd predicate không null-safe) bị tính là fail, khớp cách Deequ tính metric
    val errors = array_compact(array(rowLevelRuleIds.map { id =>
      when(!coalesce(col(DeequChecks.columnName(id)), lit(false)), lit(id))
    }: _*))
    // bỏ mọi cột của Deequ, kể cả rule có hỗ trợ row-level nhưng không đánh dấu row_level
    val deequColumns = withRowLevel.columns.filter(_.startsWith(DeequChecks.ColumnPrefix))
    val tagged = withRowLevel.withColumn(ErrorsColumn, errors).drop(deequColumns: _*)

    val isBad = size(col(ErrorsColumn)) > 0
    val counts = tagged.agg(
      count(when(isBad, 1)).as("_quarantined"),
      rowLevelRuleIds.map(id => count(when(array_contains(col(ErrorsColumn), id), 1)).as(id)): _*
    ).head()

    val quarantinedRows = counts.getLong(0)
    val failedByRule = rowLevelRuleIds.zipWithIndex.map { case (id, i) => id -> counts.getLong(i + 1) }.toMap
    Split(
      valid = tagged.where(!isBad).drop(ErrorsColumn),
      quarantine = if (quarantinedRows > 0) Some(tagged.where(isBad)) else None,
      quarantinedRows = quarantinedRows,
      failedRowsByRule = failedByRule)
  }
}
