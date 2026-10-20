# spark (image, cấu hình và job Python)

Mọi thứ phía Spark (data plane): image chạy cả job Python lẫn Scala, cấu hình `spark.dq.*`, và các job Python mẫu.

```
spark/
├── Dockerfile                 # stage 1: mvn package ra dq-platform.jar; stage 2: apache/spark 3.5.1 + jar
├── conf/spark-defaults.conf   # spark.jars + các tham số spark.dq.*
└── jobs/
    ├── dq_client.py           # wrapper py4j của dq-client (JVM)
    ├── bronze_to_silver.py    # job Python: transform -> audit -> quarantine -> publish
    └── generate_bronze.py     # sinh dữ liệu Bronze mẫu có lỗi cố ý
```

Logic DQ **không** nằm ở đây. Nó nằm trong [jvm/dq-client](../jvm/dq-client/README.md); phía Python chỉ gọi vào. Job Scala tương đương ở [jvm/dq-jobs](../jvm/dq-jobs/README.md).

## Dockerfile

Build context phải là **thư mục gốc repo** (không phải `spark/`), vì stage 1 cần `jvm/` và `suites/` (test đọc suite thật):

```bash
docker build -f spark/Dockerfile -t dq-demo/spark-deequ:3.5.1 .
```

| Stage | Làm gì |
|---|---|
| `build` (`maven:3.9-eclipse-temurin-17`) | `mvn package` (gồm test) ra `dq-platform.jar` |
| cuối (`apache/spark:3.5.1-scala2.12-java17-python3-ubuntu`) | Chép jar vào `/opt/dq/jars/` |

Không cần `pip install` gì: Deequ chạy trong JVM, executor không cần package Python nào. Driver và executor dùng chung image.

## spark-defaults.conf

| Config | Ý nghĩa |
|---|---|
| `spark.jars` | `/opt/dq/jars/dq-platform.jar`: client DQ cho job Python. Thiếu thì `DQClient(spark)` báo lỗi "không tìm thấy dqplatform.client.DQClient" |
| `spark.dq.service.url` | Địa chỉ DQ service |
| `spark.dq.mode` | `enforce` hoặc `monitor` |
| `spark.dq.fail.on.service.error` | Fail-closed (`true`) hay fail-open (`false`) |
| `spark.dq.service.timeout.sec` | Timeout HTTP |
| `spark.dq.quarantine.path`, `spark.dq.staging.path` | Nơi ghi dòng lỗi và vùng staging của WAP |

`spark.dq.dataset` do từng job đặt qua `--conf`. Token: biến môi trường `DQ_API_TOKEN` của driver.

Bảng đầy đủ về từng config và ý nghĩa fail-open / fail-closed ở [README gốc](../README.md#cấu-hình-job-spark).

## Job Python

### `dq_client.py`

Giữ nguyên API cũ để job không phải đổi:

```python
dq = DQClient(spark)
res = dq.validate(df, partition_spec="dt=2026-10-17", suite_parameters={...})
res.verdict, res.reasons, res.valid_df, res.quarantine_df, res.should_publish
res.release()     # gỡ cache; gọi sau khi ghi xong valid_df / quarantine_df
```

`DQFailed` được ném khi DQ service lỗi ở chế độ fail-closed (hoặc bị từ chối bằng 4xx). `quarantine_df` có cột `_dq_errors`: danh sách id rule bị vi phạm.

### `bronze_to_silver.py`

Cùng luồng với job Scala; ghi vào `silver/transactions` và `quarantine/transactions`. Quarantine và publish gọi `dqplatform.jobs.Wap` trong jar qua py4j, nên hai job dùng đúng một code ghi. Exit code: `0` đã publish, `2` DQ service lỗi, `3` verdict FAIL.

Transform phải khớp với [TransactionsTransform.scala](../jvm/dq-jobs/src/main/scala/dqplatform/jobs/TransactionsTransform.scala): sửa một bên thì sửa cả hai, rồi chạy [scripts/parity_check.py](../scripts/parity_check.py).

### `generate_bronze.py`

Sinh Bronze có replay (50 dòng gửi lại) và lỗi cố ý mỗi loại ứng với một dimension: thiếu `subscriber_id` và `amount` không ép được (completeness), msisdn / amount âm / status sai (validity), `event_time` sai ngày (consistency).

```bash
spark-submit generate_bronze.py --out /data/lake/bronze/transactions --dt 2026-10-17 --rows 10000 --bad-ratio 0.03
```

`--bad-ratio 0.12` để thử kịch bản FAIL (quarantine vượt 5%, job exit 3). `--seed` mặc định 42, và số liệu baseline trong tài liệu dựa trên seed này.

## Chạy job Python không dùng Docker

```bash
export DQ_API_TOKEN=<token>
spark-submit --master "local[2]" \
  --jars ../jvm/dq-jobs/target/dq-platform.jar --py-files jobs/dq_client.py \
  --conf spark.dq.service.url=http://127.0.0.1:8000 --conf spark.dq.dataset=silver.transactions \
  jobs/bronze_to_silver.py --dt 2026-10-17 --lake /tmp/lake
```

Cần build jar trước (`mvn -f ../jvm/pom.xml package`) và có DQ service đang chạy.
