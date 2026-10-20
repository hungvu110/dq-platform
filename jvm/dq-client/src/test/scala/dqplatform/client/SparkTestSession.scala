package dqplatform.client

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, Suite => TestSuite}

/** SparkSession local dùng chung cho các spec (mỗi JVM test một session). */
trait SparkTestSession extends BeforeAndAfterAll { self: TestSuite =>

  lazy val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName(getClass.getSimpleName)
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "1")
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh")
    .getOrCreate()

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sparkContext.setLogLevel("WARN")
  }
}
