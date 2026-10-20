# dq-jobs (job Spark Scala và jar triển khai)

Module này làm hai việc:

1. Chứa job Scala `BronzeToSilverTransactions`, bản tương đương của [bronze_to_silver.py](../../spark/jobs/bronze_to_silver.py).
2. Đóng gói **`dq-platform.jar`** bằng `maven-shade-plugin`: chứa `dq-client`, job và Deequ. Spark, Scala và Jackson là `provided` nên không nằm trong jar. Job Python cũng nạp jar này (`spark.jars`) để dùng client.

Thư viện DQ mà job dùng: [dq-client](../dq-client/README.md).

## Job `BronzeToSilverTransactions`

Write-Audit-Publish cho bảng giao dịch:

```
đọc Bronze -> transform -> AUDIT (DQClient.validate) -> ghi quarantine -> WRITE staging -> PUBLISH (rename)
```

```bash
spark-submit --class dqplatform.jobs.BronzeToSilverTransactions \
  --conf spark.dq.service.url=http://dq-service:8000 \
  --conf spark.dq.dataset=silver.transactions \
  dq-platform.jar --dt 2026-10-17 --lake /data/lake
```

| Tham số | Mặc định | Ý nghĩa |
|---|---|---|
| `--dt` | bắt buộc | Ngày partition, `yyyy-MM-dd` |
| `--lake` | `/data/lake` | Gốc của lake |
| `--target-layer` | `silver_scala` | Tên layer đích |

Job ghi vào thư mục **riêng** để chạy song song với job Python:

| | Đường dẫn |
|---|---|
| Đọc | `<lake>/bronze/transactions/dt=...` |
| Silver | `<lake>/silver_scala/transactions/dt=...` |
| Quarantine | `<spark.dq.quarantine.path>/transactions_scala/dt=...` (mặc định `<lake>/quarantine`) |
| Staging | `<spark.dq.staging.path>/transactions_scala/<run_id>` (mặc định `<lake>/_staging`) |

### Exit code (hợp đồng với Airflow)

| Code | Ý nghĩa |
|---|---|
| `0` | Đã publish (verdict PASS, WARN hoặc SKIPPED) |
| `2` | DQ service lỗi ở chế độ fail-closed, hoặc bị từ chối (4xx) |
| `3` | Verdict FAIL: không publish, task fail và chặn downstream |

## Các file

| File | Vai trò |
|---|---|
| [BronzeToSilverTransactions.scala](src/main/scala/dqplatform/jobs/BronzeToSilverTransactions.scala) | `main`, và `run(spark, args, dq)` nhận `DQClient` từ ngoài để test được |
| [TransactionsTransform.scala](src/main/scala/dqplatform/jobs/TransactionsTransform.scala) | Chuẩn hóa, dedup, và `sourceRowCount` |
| [Wap.scala](src/main/scala/dqplatform/jobs/Wap.scala) | `publish` và `writeQuarantine`, dùng chung cho job Python (qua py4j) |

### Transform phải khớp job Python

Cả hai job phải cho cùng kết quả (kiểm bằng [scripts/parity_check.py](../../scripts/parity_check.py)). Sửa transform ở một bên thì sửa cả bên kia:

- bỏ khoảng trắng đầu cuối rồi đổi đầu `0` của msisdn thành `84`;
- ép `amount` thành `decimal(18,2)`, đổi `status` sang chữ hoa, ép `event_time` thành timestamp;
- dedup theo `transaction_id`, giữ bản có `ingest_time` mới nhất;
- dòng thiếu `transaction_id` được **giữ lại** (không gộp làm một) để rule completeness đưa vào quarantine.

`sourceRowCount` được tính thẳng từ Bronze, độc lập với transform, để rule `row_count_reconcile` đối soát được việc transform làm mất hay nhân bản giao dịch.

### Suite parameter job truyền

| Parameter | Giá trị |
|---|---|
| `partition_start` | `<dt> 00:00:00` |
| `partition_end` | `<dt+1> 00:00:00` |
| `source_row_count` | Số giao dịch ở Bronze |

Job chỉ truyền dữ kiện; ngưỡng nằm trong suite ở registry.

## `Wap`: vì sao publish như vậy

- **Quarantine** thay nguyên partition `dt` rồi ghi mới, nên chạy lại cùng `dt` (Airflow retry) không nhân bản dòng lỗi. Partition được thay kể cả khi lần này không có dòng lỗi.
- **Publish** ghi staging, đổi partition cũ sang `.bak-<run_id>`, rename staging vào chỗ, rồi xóa `.bak`. Rename lỗi thì khôi phục bản cũ, Silver không bao giờ bị mất. Staging luôn được dọn.
- Rename atomic là tính chất của HDFS. Trên S3 thì không; Iceberg (branch / snapshot) là cách đúng ở đó.

## Build và test

```bash
cd jvm
mvn -B package          # test cả hai module, rồi tạo dq-jobs/target/dq-platform.jar
```

`BronzeToSilverTransactionsSpec` chạy toàn bộ job với service giả và lake tạm: publish, FAIL, service sập, chạy lại cùng `dt`, đọc tham số.

## Thêm một job mới

1. Tạo object trong `dqplatform.jobs`, viết `run(spark, args, dq): Int` giống job này để test được.
2. Dùng `DQClient.fromSession(spark).validate(...)`, `Wap.writeQuarantine` và `Wap.publish`; giữ đúng exit code 0 / 2 / 3.
3. Thêm suite cho dataset vào [suites/](../../suites) (xem [RULE_SPEC_V2.md](../../docs/RULE_SPEC_V2.md)).
4. Thêm một service vào [docker-compose.yml](../../docker-compose.yml) nếu muốn chạy thử.
