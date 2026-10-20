"""
Bronze -> Silver cho bảng transactions, có DQ gate theo Write-Audit-Publish:
  transform -> AUDIT (dq_client.validate) -> WRITE staging -> PUBLISH (rename vào partition Silver)
Dòng lỗi -> quarantine. Verdict FAIL + mode=enforce -> không publish, exit code != 0.
Suite silver.transactions chia rule theo 6 dimension; job cung cấp suite parameter cho các rule
completeness (đối soát số dòng), consistency (biên partition) và timeliness (freshness).
"""
import argparse
import sys
from datetime import date, timedelta

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from dq_client import DQClient, DQFailed

p = argparse.ArgumentParser()
p.add_argument("--dt", required=True)
p.add_argument("--lake", default="/data/lake")
a = p.parse_args()

spark = SparkSession.builder.appName("bronze_to_silver_transactions").getOrCreate()
conf = spark.conf
bronze = f"{a.lake}/bronze/transactions/dt={a.dt}"
silver = f"{a.lake}/silver/transactions/dt={a.dt}"
quarantine_root = conf.get("spark.dq.quarantine.path", f"{a.lake}/quarantine")
staging_root = conf.get("spark.dq.staging.path", f"{a.lake}/_staging")

# ---------- 1. Transform (làm sạch, chuẩn hóa) ----------
raw = spark.read.parquet(bronze)
w = Window.partitionBy("transaction_id").orderBy(F.col("ingest_time").desc())
df = (raw
      .withColumn("msisdn", F.regexp_replace(F.trim("msisdn"), r"^0", "84"))
      .withColumn("amount", F.col("amount").cast("decimal(18,2)"))
      .withColumn("status", F.upper(F.trim("status")))
      .withColumn("event_time", F.to_timestamp("event_time"))
      .withColumn("_rn", F.row_number().over(w))
      # dedup Kafka replay. Dòng thiếu transaction_id giữ nguyên (không gộp làm một)
      # để rule completeness đưa vào quarantine thay vì mất im lặng.
      .where("_rn = 1 OR transaction_id IS NULL")
      .drop("_rn", "ingest_time")
      .withColumn("dt", F.lit(a.dt)))

# ---------- 2. Audit ----------
# Dữ kiện runtime cho suite (${x} trong SQL, {"param": x} trong assert); ngưỡng nằm trong suite, không nằm ở đây.
# source_row_count tính thẳng từ Bronze, độc lập với logic transform -> rule đối soát completeness.
src = raw.agg(F.countDistinct("transaction_id").alias("keys"),
              F.count(F.when(F.col("transaction_id").isNull(), 1)).alias("no_key")).first()
suite_params = {
    "partition_start": f"{a.dt} 00:00:00",                                   # consistency
    "partition_end": f"{date.fromisoformat(a.dt) + timedelta(days=1)} 00:00:00",  # consistency, timeliness
    "source_row_count": src["keys"] + src["no_key"],                         # completeness
}

dq = DQClient(spark)
try:
    res = dq.validate(df, partition_spec=f"dt={a.dt}", suite_parameters=suite_params)
except DQFailed as e:
    print(f"DQ ERROR: {e}", file=sys.stderr)
    sys.exit(2)

# Ghi quarantine / publish dùng chung code JVM với job Scala (dqplatform.jobs.Wap trong dq-platform.jar):
#  - quarantine thay nguyên partition dt -> chạy lại / Airflow retry không nhân bản dòng lỗi
#  - publish: bản cũ đổi sang .bak, chỉ xóa khi bản mới đã vào chỗ; rename lỗi thì khôi phục
wap = spark._jvm.dqplatform.jobs.Wap
try:
    wap.writeQuarantineNullable(res.quarantine_df._jdf if res.quarantine_df is not None else None,
                                res.run_id, f"{quarantine_root}/transactions", a.dt,
                                spark._jsc.hadoopConfiguration())
    if not res.should_publish:
        print(f"DQ {res.verdict}: không publish {silver}. Lý do: {res.reasons}", file=sys.stderr)
        sys.exit(3)                                                  # Airflow task fail -> chặn downstream

    # ---------- 3. Write staging -> 4. Publish ----------
    wap.publish(res.valid_df.drop("dt")._jdf, f"{staging_root}/transactions/{res.run_id}", silver)
    print(f"DQ {res.verdict}: published {res.row_count - res.quarantined_rows} rows -> {silver} "
          f"(quarantined {res.quarantined_rows}, run {res.run_id})")
finally:
    res.release()
