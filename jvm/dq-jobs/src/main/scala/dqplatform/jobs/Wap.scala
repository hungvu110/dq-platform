package dqplatform.jobs

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{current_timestamp, lit}
import org.apache.spark.sql.DataFrame

/** Write-Audit-Publish: ghi staging rồi rename vào partition đích. */
object Wap {

  /**
   * df đã bỏ cột partition (dt nằm trong tên thư mục đích).
   * Partition cũ được đổi tên sang .bak trước, chỉ xóa khi bản mới đã vào chỗ -> rename lỗi thì
   * khôi phục bản cũ, Silver không bao giờ bị mất. Staging luôn được dọn.
   */
  def publish(df: DataFrame, staging: String, target: String): Unit = {
    val stagingPath = new Path(staging)
    val targetPath = new Path(target)
    val backupPath = new Path(s"$target.bak-${stagingPath.getName}")
    val fs = targetPath.getFileSystem(df.sparkSession.sparkContext.hadoopConfiguration)
    try {
      df.write.mode("overwrite").parquet(staging)
      val hadOld = fs.exists(targetPath)
      if (hadOld && !fs.rename(targetPath, backupPath))
        throw new IllegalStateException(s"publish failed: không đổi được $target -> $backupPath")
      fs.mkdirs(targetPath.getParent)
      if (!fs.rename(stagingPath, targetPath)) {
        if (hadOld) fs.rename(backupPath, targetPath)
        throw new IllegalStateException(s"publish failed: rename $staging -> $target (đã khôi phục bản cũ)")
      }
      if (hadOld) fs.delete(backupPath, true)
    } finally {
      if (fs.exists(stagingPath)) fs.delete(stagingPath, true)
    }
  }

  /**
   * Dòng lỗi (đã có cột _dq_errors) -> quarantine, kèm run id để truy vết ngược về dq_runs.
   * Thay nguyên partition dt (kể cả khi lần này không có dòng lỗi) -> chạy lại / Airflow retry không nhân bản dòng.
   */
  def writeQuarantine(quarantine: Option[DataFrame], runId: String, root: String, dt: String,
                      fsConf: org.apache.hadoop.conf.Configuration): Unit = {
    val partition = new Path(s"$root/dt=$dt")
    val fs = partition.getFileSystem(fsConf)
    if (fs.exists(partition)) fs.delete(partition, true)
    quarantine.foreach(
      _.withColumn("_dq_run_id", lit(runId))
        .withColumn("_dq_quarantined_at", current_timestamp())
        .write.mode("append").partitionBy("dt").parquet(root))
  }

  /** Cho job Python (py4j không có Option): quarantine = null khi không có dòng lỗi. */
  def writeQuarantineNullable(quarantine: DataFrame, runId: String, root: String, dt: String,
                              fsConf: org.apache.hadoop.conf.Configuration): Unit =
    writeQuarantine(Option(quarantine), runId, root, dt, fsConf)
}
