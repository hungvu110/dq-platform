"""Sinh dữ liệu Bronze mẫu, cố ý trộn lỗi để thấy DQ gate hoạt động."""

import argparse
import random
from datetime import datetime, timedelta

from pyspark.sql import SparkSession

p = argparse.ArgumentParser()
p.add_argument("--out", required=True)
p.add_argument("--dt", required=True)
p.add_argument("--rows", type=int, default=10000)
p.add_argument("--bad-ratio", type=float, default=0.03)
p.add_argument("--seed", type=int, default=42)
a = p.parse_args()

random.seed(a.seed)
base = datetime.fromisoformat(a.dt)
STATUS = [
    "success",
    "SUCCESS",
    " Failed ",
    "REFUNDED",
]  # lẫn hoa/thường, khoảng trắng -> job chuẩn hóa
rows = []
for i in range(a.rows):
    r = {
        "transaction_id": f"TX{a.dt.replace('-', '')}{i:07d}",
        "subscriber_id": f"SUB{random.randint(1, 50000):06d}",
        "msisdn": random.choice(["0", "84"])
        + f"9{random.randint(10000000, 99999999)}",
        "amount": str(
            random.choice([10000, 20000, 50000, 100000, 200000, 500000])
        ),
        "status": random.choice(STATUS),
        "province_code": (
            random.choice(["HN", "HCM", "DN", "HP", None])
            if random.random() > 0.02
            else None
        ),
        "event_time": (
            base + timedelta(seconds=random.randint(0, 86399))
        ).isoformat(sep=" "),
        "ingest_time": (
            base + timedelta(days=1, minutes=random.randint(0, 30))
        ).isoformat(sep=" "),
    }
    if random.random() < a.bad_ratio:  # lỗi cố ý
        kind = random.choice(
            [
                "null_sub",  # completeness
                "bad_amount",  # completeness: cast decimal ra null
                "bad_msisdn",  # validity
                "neg_amount",  # validity
                "bad_status",  # validity
                "wrong_day",  # consistency: event_time không thuộc partition dt
            ]
        )
        if kind == "null_sub":
            r["subscriber_id"] = None
        elif kind == "bad_amount":
            r["amount"] = "50k"
        elif kind == "bad_msisdn":
            r["msisdn"] = "12345"
        elif kind == "neg_amount":
            r["amount"] = "-50000"
        elif kind == "bad_status":
            r["status"] = "PENDING???"
        else:
            r["event_time"] = (
                base - timedelta(seconds=random.randint(1, 86400))
            ).isoformat(sep=" ")
    rows.append(r)
rows += [
    dict(
        rows[i],
        ingest_time=(base + timedelta(days=1, hours=2)).isoformat(sep=" "),
    )
    for i in range(0, 50)
]  # 50 bản ghi bị gửi lại (Kafka replay)

spark = SparkSession.builder.appName("generate_bronze").getOrCreate()
(
    spark.createDataFrame(rows)
    .coalesce(1)
    .write.mode("overwrite")
    .parquet(f"{a.out}/dt={a.dt}")
)
print(f"wrote {len(rows)} rows to {a.out}/dt={a.dt}")
