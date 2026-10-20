"""
Parity check (docs/DEEQU_MIGRATION_PLAN.md, Phase 5): job Python và job Scala chạy trên cùng Bronze phải
cho cùng kết quả, và khớp baseline (mặc định = số liệu của bản GX trên dữ liệu mẫu seed 42).

Chạy sau khi cả hai job đã chạy cho cùng --dt:
  spark-submit scripts/parity_check.py --lake /data/lake --dt 2026-10-17 --service-url http://dq-service:8000
Exit 0 = khớp, 1 = lệch (in chi tiết).
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

p = argparse.ArgumentParser()
p.add_argument("--lake", default="/data/lake")
p.add_argument("--quarantine", default=None, help="mặc định <lake>/quarantine")
p.add_argument("--dt", required=True)
p.add_argument("--dataset", default="silver.transactions")
p.add_argument("--service-url", default="http://dq-service:8000")
p.add_argument("--token", default=os.environ.get("DQ_API_TOKEN"), help="mặc định env DQ_API_TOKEN")
p.add_argument("--expect-rows", type=int, default=10000)
p.add_argument("--expect-quarantined", type=int, default=308)
p.add_argument("--expect-verdict", default="WARN")
a = p.parse_args()
if not a.token:
    p.error("cần --token hoặc env DQ_API_TOKEN")

quarantine_root = a.quarantine or f"{a.lake}/quarantine"
problems: list[str] = []


def check(cond: bool, msg: str) -> None:
    print(("OK   " if cond else "FAIL ") + msg)
    if not cond:
        problems.append(msg)


def api(path: str):
    req = urllib.request.Request(a.service_url.rstrip("/") + path, headers={"Authorization": f"Bearer {a.token}"})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def same_rows(left: DataFrame, right: DataFrame) -> bool:
    return left.exceptAll(right).isEmpty() and right.exceptAll(left).isEmpty()


# ---------- 1. Kết quả run ở DQ service: lần chạy mới nhất của mỗi client cho partition ----------
runs = [r for r in api(f"/api/v1/runs?dataset={a.dataset}&limit=200") if r["partition_spec"] == f"dt={a.dt}"]
latest = {}
for r in runs:  # API trả mới nhất trước
    latest.setdefault(r["client"], r)
check({"python", "scala"} <= latest.keys(), f"có run của cả python và scala cho dt={a.dt}: {sorted(latest)}")
if problems:
    sys.exit(1)
py, sc = latest["python"], latest["scala"]
for key in ("verdict", "row_count", "quarantined_rows", "reasons", "success_percent"):
    check(py[key] == sc[key], f"run.{key}: python={py[key]} scala={sc[key]}")
check(py["verdict"] == a.expect_verdict, f"verdict = baseline {a.expect_verdict}")
check(py["row_count"] == a.expect_rows, f"row_count = baseline {a.expect_rows}")
check(py["quarantined_rows"] == a.expect_quarantined, f"quarantined_rows = baseline {a.expect_quarantined}")

fields = ("status", "metric_value", "failed_rows")
rules_py = {x["rule_id"]: x for x in api(f"/api/v1/runs/{py['run_id']}/rules")}
rules_sc = {x["rule_id"]: x for x in api(f"/api/v1/runs/{sc['run_id']}/rules")}
diff = [rid for rid in rules_py.keys() | rules_sc.keys()
        if tuple(rules_py.get(rid, {}).get(f) for f in fields) != tuple(rules_sc.get(rid, {}).get(f) for f in fields)]
check(not diff, f"{len(rules_py)} rule cho cùng status/metric/failed_rows" + (f", lệch: {sorted(diff)}" if diff else ""))

# ---------- 2. Dữ liệu: Silver và quarantine của hai job ----------
spark = SparkSession.builder.appName("dq_parity_check").getOrCreate()
silver_py = spark.read.parquet(f"{a.lake}/silver/transactions/dt={a.dt}")
silver_sc = spark.read.parquet(f"{a.lake}/silver_scala/transactions/dt={a.dt}")
check(silver_py.count() == a.expect_rows - a.expect_quarantined,
      f"Silver python có {silver_py.count()} dòng (baseline {a.expect_rows - a.expect_quarantined})")
check(same_rows(silver_py, silver_sc.select(*silver_py.columns)), "Silver python = Silver scala (từng dòng)")


def quarantine_of(path: str, run_id: str) -> DataFrame:
    return (spark.read.parquet(path).where(F.col("_dq_run_id") == run_id)
            .withColumn("_dq_errors", F.array_sort("_dq_errors")).drop("_dq_run_id", "_dq_quarantined_at"))


q_py = quarantine_of(f"{quarantine_root}/transactions", py["run_id"])
q_sc = quarantine_of(f"{quarantine_root}/transactions_scala", sc["run_id"])
check(q_py.count() == a.expect_quarantined, f"quarantine python có {q_py.count()} dòng")
check(same_rows(q_py, q_sc.select(*q_py.columns)), "quarantine python = quarantine scala (từng dòng, cùng _dq_errors)")
print("\nquarantine theo rule:")
q_py.select(F.explode("_dq_errors").alias("rule")).groupBy("rule").count().orderBy("rule").show(truncate=False)

print("PARITY OK" if not problems else f"PARITY FAILED ({len(problems)} lỗi)")
sys.exit(1 if problems else 0)
