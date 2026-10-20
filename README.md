# DQ Platform demo — Data Quality Service + Spark + Deequ

Demo kiến trúc **DQ service tách riêng** cho data platform:

- **Control plane: `dq-service`** (FastAPI + Postgres). Không chạm vào dữ liệu. Gồm rule registry (suite có version), policy engine (PASS / WARN / FAIL), result store và alerting.
- **Data plane: job Spark** + thư viện **`dq-client`** (Scala, chạy [Deequ](https://github.com/awslabs/deequ) ngay trong Spark, "compute to data"). Job Scala dùng trực tiếp, job Python gọi qua py4j, nên cả hai **dùng chung một bộ diễn giải rule**. Chỉ suite (vài KB) và kết quả (vài KB) đi qua HTTP.

Stack: Spark 3.5.1, Scala 2.12, Deequ `2.0.21-spark-3.5`, build JVM bằng Maven.

## Tài liệu theo thành phần

README này là bức tranh chung. Chi tiết từng phần nằm ở tài liệu riêng:

| Tài liệu | Đọc khi |
|---|---|
| [dq-service/README.md](dq-service/README.md) | Muốn biết API, token, cách verdict được quyết định, hoặc sửa service |
| [jvm/dq-client/README.md](jvm/dq-client/README.md) | Muốn hiểu `DQClient.validate()` làm gì, thêm một loại rule (`kind`), hoặc nâng Deequ |
| [jvm/dq-jobs/README.md](jvm/dq-jobs/README.md) | Muốn chạy hoặc viết job Spark Scala, hiểu `Wap` (publish và quarantine), hoặc build `dq-platform.jar` |
| [spark/README.md](spark/README.md) | Muốn build image, chỉnh `spark-defaults.conf`, hoặc chạy job Python và sinh dữ liệu mẫu |
| [docs/RULE_SPEC_V2.md](docs/RULE_SPEC_V2.md) | Viết hoặc sửa rule trong suite |
| [docs/DEEQU_MIGRATION_PLAN.md](docs/DEEQU_MIGRATION_PLAN.md) | Muốn biết vì sao thiết kế như vậy, các quyết định đã chốt, và kết quả nghiệm thu |

Phần việc nào thì sửa ở đúng thành phần đó, nhưng có hai cặp phải đổi cùng nhau: định dạng suite ([models.py](dq-service/models.py) và [RuleSpec.scala](jvm/dq-client/src/main/scala/dqplatform/client/RuleSpec.scala)), và phần transform của hai job ([bronze_to_silver.py](spark/jobs/bronze_to_silver.py) và [TransactionsTransform.scala](jvm/dq-jobs/src/main/scala/dqplatform/jobs/TransactionsTransform.scala)).

```
.
├── docker-compose.yml
├── postgres/
│   ├── init.sql                       # schema: dq_suites, dq_runs, dq_rule_results
│   └── migrations/001_gx_to_deequ.sql # nâng DB tạo từ bản Great Expectations
├── dq-service/                        # control plane
│   ├── app.py                         # route, auth, seed, alert
│   ├── models.py                      # rule spec v2 + payload run (Pydantic)
│   ├── policy.py                      # verdict + scorecard 6 dimension
│   ├── store.py                       # registry + result store
│   └── tests/                         # pytest
├── suites/silver_transactions.json    # suite mẫu (rule spec v2), seed vào registry khi service khởi động
├── jvm/                               # Maven multi-module
│   ├── pom.xml
│   ├── dq-client/                     # thư viện DQ dùng chung (Deequ), + test
│   └── dq-jobs/                       # job Scala BronzeToSilverTransactions, shade -> dq-platform.jar
├── spark/
│   ├── Dockerfile                     # stage 1: mvn package; stage 2: apache/spark 3.5.1 + dq-platform.jar
│   ├── conf/spark-defaults.conf       # spark.jars + các tham số spark.dq.*
│   └── jobs/
│       ├── dq_client.py               # wrapper py4j của dq-client cho job Python
│       ├── generate_bronze.py         # sinh dữ liệu bronze có lỗi cố ý
│       └── bronze_to_silver.py        # job Python: transform -> audit -> write staging -> publish
└── scripts/parity_check.py            # so kết quả job Python và job Scala
```

## Kiến trúc check data quality

Mọi job Spark ghi vào một layer (Bronze → Silver, Silver → Gold...) đều đi qua cùng một DQ gate, bất kể viết bằng Python hay Scala. Dữ liệu ở lại trong cluster, chỉ suite và kết quả đi qua mạng.

```mermaid
flowchart LR
    AF["Airflow"] -->|spark-submit| PY & SC

    subgraph SPARK["Data plane: Spark (image dùng chung)"]
        PY["Job Python<br/>bronze_to_silver.py"] -->|py4j| LIB
        SC["Job Scala<br/>BronzeToSilverTransactions"] --> LIB
        LIB["dq-client (Scala)<br/>+ Deequ"]
    end

    subgraph LAKE["Lake (HDFS / S3)"]
        IN[("Bronze")]
        OUT[("Silver")]
        QUA[("Quarantine")]
    end

    subgraph SVC["Control plane: DQ service"]
        REG["Rule registry<br/>suite theo 6 dimension"]
        POL["Policy engine<br/>PASS / WARN / FAIL"]
        RES["Result store"]
        ALR["Alerting"]
    end

    GIT["suites/*.json"] -->|PUT /suites| REG
    IN --> PY & SC
    REG -->|suite| LIB
    LIB -->|kết quả theo rule| POL
    POL -->|verdict| LIB
    POL --> RES --> PG[("Postgres")]
    POL --> ALR --> HOOK["Slack / Teams"]
    LIB -->|dòng lỗi| QUA
    LIB -->|PASS / WARN: publish| OUT
    PG --> DASH["Dashboard<br/>scorecard 6 dimension"]
```

Ghi chú:

- Job FAIL thì không publish và trả exit code khác 0, nên Airflow chặn các task phía sau.
- Chỉ **driver** gọi HTTP (GET suite, POST run). Deequ tính metric và tách quarantine phân tán trên executor; dữ liệu không rời cluster.
- Job chỉ cung cấp **dữ kiện** qua suite parameter. **Ngưỡng** (tỉ lệ tối thiểu, min/max, `TIMESTAMP '${partition_end}' - INTERVAL 60 MINUTES`...) nằm trong suite ở registry.
- Exit code là hợp đồng với Airflow: `0` là đã publish, `2` là DQ service lỗi (fail-closed), `3` là verdict FAIL.

### Luồng một lần kiểm tra (Write-Audit-Publish)

```mermaid
sequenceDiagram
    participant J as Job Spark (driver)
    participant C as dq-client + Deequ
    participant S as DQ service
    participant L as Lake

    J->>J: transform Bronze, tính suite parameter
    J->>C: validate(df, partition, parameters)
    C->>S: GET /suites/{dataset}
    S-->>C: suite (rule spec v2) + version
    C->>C: Deequ chạy mọi rule (executor), tách dòng lỗi
    C->>S: POST /runs (kết quả từng rule, không có dữ liệu)
    S->>S: decide() ra PASS / WARN / FAIL, lưu, alert
    S-->>C: verdict + reasons + scorecard
    C-->>J: DQResult (validDf, quarantineDf, verdict)
    J->>L: ghi quarantine (thay partition dt)
    alt PASS / WARN
        J->>L: ghi staging, rename vào Silver
    else FAIL
        J->>J: không publish, exit 3
    end
```

### Quyết định thiết kế chính

Lý do đầy đủ, các phương án đã loại và đánh đổi nằm ở [docs/DEEQU_MIGRATION_PLAN.md](docs/DEEQU_MIGRATION_PLAN.md) (mục 3).

| Quyết định | Lý do |
|---|---|
| **Một bộ diễn giải rule duy nhất, trong JVM.** Job Python gọi qua py4j, không dùng PyDeequ | Hai bộ dịch rule song song (Scala và Python) sẽ lệch nhau theo thời gian: cùng suite mà hai job cho verdict khác nhau. Đánh đổi: py4j không có trên Spark Connect |
| **Suite là JSON riêng (rule spec v2)**, không dùng DQDL, không giữ format Great Expectations | DQDL không gắn được `dimension` / `severity` cho từng rule và không có row-level cho regex, khoảng giá trị, SQL. Format riêng không trói service vào từ vựng của một engine |
| **Verdict do service quyết định**, client chỉ gửi kết quả từng rule | Đổi policy mà không deploy lại job; đổi engine mà service không phải sửa |
| **Mỗi rule là một Deequ Check riêng**, `description = rule.id` | Lấy được kết quả theo từng rule; cột row-level mang đúng tên rule; hiệu năng không đổi vì Deequ vẫn gộp analyzer |
| **Quarantine bằng `rowLevelResultsAsDataFrame`** | API chính thức của Deequ, thay cho việc parse chuỗi nội bộ của GX (vỡ khi nâng version) |
| **Job truyền dữ kiện, suite giữ ngưỡng** (`${x}`, `{"param": x}`) | Đổi ngưỡng không cần deploy lại job |
| **Rule schema chạy trên `df.schema` ở client** | Deequ không so khớp chính xác tập cột, và `hasDataType` chỉ suy kiểu cho cột string |

## Chạy job

Có hai cách chạy, và **mỗi cách đều chạy được cả job Python lẫn job Scala**. Cả hai job đọc cùng một Bronze, dùng cùng suite, và phải cho cùng kết quả.

| | Docker | Local |
|---|---|---|
| Cần | Docker + Compose | Java 11+, Maven, Python 3.10+ |
| Spark | Trong image (3.5.1) | `pyspark==3.5.1` |
| Postgres | Có | Không (sqlite) |
| Build jar | Tự động trong Dockerfile (kèm test) | `mvn package` |
| Đã kiểm | **Chưa** (daemon không chạy trên máy dev; compose đã qua `docker compose config`) | **Rồi**, đầu-cuối cả hai job |

### Cách 1: Docker

```bash
# 1. Postgres + DQ service. Lần đầu build image (mvn package, gồm test) nên lâu
docker compose up -d --build postgres dq-service

# 2. Sinh Bronze mẫu: 10.000 dòng + 50 dòng replay, ~3% lỗi
docker compose run --rm spark-gen-data

# 3a. Job Python  -> silver/transactions
docker compose run --rm spark-bronze-to-silver

# 3b. Job Scala   -> silver_scala/transactions
docker compose run --rm spark-bronze-to-silver-scala

# 4. So kết quả hai job với nhau và với baseline (in PARITY OK nếu khớp)
docker compose run --rm spark-parity-check
```

Chạy ngày khác hoặc tỉ lệ lỗi khác bằng biến môi trường, áp dụng cho mọi service ở trên:

```bash
# Kịch bản FAIL: 12% lỗi vượt ngưỡng quarantine 5%, cả hai job không publish và exit 3.
# Bước 4 không áp dụng vì không có Silver để so; xem exit code và lý do ở bước dưới.
DT=2026-10-18 BAD_RATIO=0.12 docker compose run --rm spark-gen-data
DT=2026-10-18 docker compose run --rm spark-bronze-to-silver;       echo "exit=$?"   # 3
DT=2026-10-18 docker compose run --rm spark-bronze-to-silver-scala; echo "exit=$?"   # 3
```

Xem kết quả và dọn dẹp:

```bash
# Lịch sử run, kết quả từng rule của một run
curl -s -H "Authorization: Bearer changeme-dev-token" "localhost:8000/api/v1/runs?dataset=silver.transactions" | jq
curl -s -H "Authorization: Bearer changeme-dev-token" "localhost:8000/api/v1/runs/<run_id>/rules" | jq

# Scorecard 6 dimension của các run gần nhất (Grafana/Superset đọc bảng này)
docker compose exec postgres psql -U dq -d dq -c \
  "select r.client, x.dimension, count(*) filter (where x.status = 'passed') as passed, count(*) as total
     from dq_rule_results x join dq_runs r using (run_id)
    where r.created_at > now() - interval '1 day' group by 1, 2 order by 1, 2"

docker compose down        # giữ dữ liệu (volume pgdata, lake)
docker compose down -v     # xóa luôn Postgres và lake, về trạng thái ban đầu
```

Lake nằm trong volume Docker `lake`, nên chạy lại cùng `DT` sẽ thay Silver và quarantine của ngày đó (không nhân bản). Sinh Bronze lại cho cùng `DT` thì ghi đè.

### Kết quả kỳ vọng (cả hai cách)

Với dữ liệu mẫu, **giống nhau cho job Python và job Scala**:

- 10.050 dòng giảm còn 10.000 sau dedup, khớp `source_row_count` đối soát từ Bronze;
- 308 dòng vào quarantine (3,08%, dưới ngưỡng 5%);
- verdict **WARN** vì `province_code` null nhiều hơn 10%;
- job vẫn publish 9.692 dòng, exit 0.

Với `BAD_RATIO=0.12` (hoặc `--bad-ratio 0.12` khi chạy local): quarantine 12,4% vượt ngưỡng 5%, nên job không publish và trả exit code 3.

Job Scala ghi vào **thư mục riêng** để chạy song song với job Python:

| | Job Python | Job Scala |
|---|---|---|
| Silver | `<lake>/silver/transactions/dt=...` | `<lake>/silver_scala/transactions/dt=...` |
| Quarantine | `<quarantine>/transactions` | `<quarantine>/transactions_scala` |
| Run trong service | `client = python` | `client = scala` |

### Cách 2: Local, không cần Docker

Chạy bằng Spark local (`pyspark`) với `dq-platform.jar` và dq-service trên sqlite. Đã kiểm với Java 21, Python 3.11, `pyspark==3.5.1`, Maven 3.8. Điều kiện: Java 11 trở lên, Maven, Python 3.10+.

```bash
# 0. Môi trường (một lần)
python3 -m venv .venv && . .venv/bin/activate
pip install "pyspark==3.5.1" -r dq-service/requirements.txt
mvn -f jvm/pom.xml -B package            # test + tạo jvm/dq-jobs/target/dq-platform.jar

export DQ_API_TOKEN=dev-token DQ_ADMIN_TOKEN=dev-admin
export PYSPARK_PYTHON=$(which python) SPARK_LOCAL_IP=127.0.0.1
LAKE=/tmp/dq-lake JAR=$PWD/jvm/dq-jobs/target/dq-platform.jar DT=2026-10-17
SUBMIT="spark-submit --master local[2] --conf spark.ui.enabled=false \
  --conf spark.sql.session.timeZone=Asia/Ho_Chi_Minh \
  --conf spark.dq.service.url=http://127.0.0.1:8000 --conf spark.dq.dataset=silver.transactions"

# 1. DQ service (terminal riêng, hoặc thêm & ở cuối). Lần đầu tự seed suite từ suites/
(cd dq-service && DATABASE_URL=sqlite:////tmp/dq.db SEED_DIR=../suites uvicorn app:app --port 8000)

# 2. Sinh Bronze mẫu
spark-submit --master local[2] spark/jobs/generate_bronze.py \
  --out $LAKE/bronze/transactions --dt $DT --rows 10000 --bad-ratio 0.03

# 3a. Job Python  -> $LAKE/silver/transactions
$SUBMIT --jars $JAR --py-files spark/jobs/dq_client.py spark/jobs/bronze_to_silver.py --dt $DT --lake $LAKE

# 3b. Job Scala   -> $LAKE/silver_scala/transactions
$SUBMIT --class dqplatform.jobs.BronzeToSilverTransactions $JAR --dt $DT --lake $LAKE

# 4. So kết quả hai job với nhau và với baseline
spark-submit --master local[2] scripts/parity_check.py --lake $LAKE --dt $DT --service-url http://127.0.0.1:8000
```

Kịch bản FAIL: lặp lại bước 2, 3a, 3b với `DT=2026-10-18` và `--bad-ratio 0.12`, cả hai job exit 3. Mỗi lần sinh Bronze mới nên dùng `DT` khác hoặc xóa `$LAKE`. Kết quả mong đợi: xem "Kết quả kỳ vọng" ở trên; `parity_check.py` in `PARITY OK`.

Lưu ý khi chạy local:

- `spark-submit` của pip không đọc `spark/conf/spark-defaults.conf`, nên quarantine và staging dùng mặc định `<lake>/quarantine` và `<lake>/_staging`, và jar phải truyền bằng `--jars`.
- Thiếu `PYSPARK_PYTHON` thì `spark-submit` có thể không tìm thấy pyspark.
- Test không cần service hay dữ liệu: `mvn -f jvm/pom.xml test` và `pytest dq-service/tests`.

### Cập nhật rule (không cần deploy lại job)

```bash
curl -X PUT -H "Authorization: Bearer changeme-admin-token" -H "Content-Type: application/json" \
  localhost:8000/api/v1/suites/silver.transactions \
  -d "$(jq '{suite, policy}' suites/silver_transactions.json)"
# -> {"dataset":"silver.transactions","version":2}
```

Service kiểm suite trước khi lưu: kind, dimension, parameter đã khai báo, regex, rule id trùng. Suite sai bị trả về 422 ngay, không lọt xuống job.

Service dùng **hai token**, cả hai bắt buộc (service không start nếu thiếu):

| Token | Ai giữ | Quyền |
|---|---|---|
| `DQ_API_TOKEN` | Job Spark | Đọc suite, gửi run, xem kết quả |
| `DQ_ADMIN_TOKEN` | CI của repo rule | Thêm `PUT /suites` |

Lý do tách: `sql_predicate` và `sql_metric` trong suite được chạy như Spark SQL trên cluster, nên ai sửa được rule là chạy được SQL. Giá trị mặc định trong `docker-compose.yml` chỉ dùng cho demo.

Khi service đã có suite cho dataset, run gửi lên phải có kết quả cho **mọi** rule của suite. Rule bị thiếu kết quả được tính là fail theo severity của nó, nên client lỗi không thể ra PASS bằng cách bỏ sót rule.

### Nâng DB đang chạy từ bản Great Expectations

`init.sql` chỉ chạy khi volume Postgres còn trống. DB cũ chạy migration rồi restart service để seed lại suite:

```bash
docker compose exec -T postgres psql -U dq -d dq -v ON_ERROR_STOP=1 < postgres/migrations/001_gx_to_deequ.sql
docker compose restart dq-service
```

Bảng `dq_expectation_results` cũ được giữ lại làm lịch sử (chỉ đọc). Kết quả mới nằm ở `dq_rule_results`.

## Quy ước trong suite

Chi tiết đầy đủ ở [docs/RULE_SPEC_V2.md](docs/RULE_SPEC_V2.md). Tóm tắt:

| Field của rule | Ý nghĩa |
|---|---|
| `id` | Tên rule, duy nhất trong suite. Là giá trị trong cột `_dq_errors` của quarantine |
| `dimension` | **Bắt buộc**, là một trong 6 data quality dimension bên dưới |
| `severity: error` | Rule fail thì batch FAIL |
| `severity: warn` | Rule fail thì batch WARN, vẫn publish và có alert |
| `row_level: true` | Dòng vi phạm bị tách vào quarantine. Rule chỉ làm FAIL batch nếu có ngưỡng riêng < 1 (vd `>= 0.97`) và bị vượt; ngoài ra do `policy.max_quarantine_ratio` quyết định |
| `check.kind` | `size`, `complete`, `completeness`, `unique`, `pattern`, `range`, `in_set`, `mean`, `sql_predicate`, `sql_metric`, `schema_columns`, `schema_type` |

### Các loại rule (`check.kind`) và Deequ tương ứng

| kind | Deequ | Row-level |
|---|---|---|
| `size` | `hasSize` | không |
| `complete`, `completeness` | `isComplete`, `hasCompleteness` | có |
| `unique` | `isUnique` / `hasUniqueness` | có |
| `pattern` | `hasPattern` + `where(col IS NOT NULL)` | có |
| `range`, `in_set` | `isContainedIn` | có |
| `mean` | `hasMean` | không |
| `sql_predicate` | `satisfies` | có |
| `sql_metric` | `customSql`, đọc từ temp view `${batch}` | không |
| `schema_columns`, `schema_type` | không qua Deequ, client so `df.schema` | không |

**Null:** mỗi rule chỉ đo một việc; null là việc của rule completeness. `pattern`, `range`, `in_set` bỏ qua null (Deequ mặc định coi null trong `hasPattern` là không khớp, client chỉnh lại cho khớp quy ước này). `sql_predicate` phải tự viết null-safe, ví dụ `event_time IS NULL OR (...)`, vì predicate trả về null bị tính là fail. Các hành vi này được ghim bằng `DeequSemanticsSpec`; chi tiết ở [docs/RULE_SPEC_V2.md](docs/RULE_SPEC_V2.md).

### 6 data quality dimensions trong `silver.transactions`

| Dimension | Câu hỏi | Rule |
|---|---|---|
| `completeness` | Có đủ dữ liệu không? | `row_count_min` ≥ 1000; `row_count_reconcile` = `source_row_count` (đối soát với Bronze); `*_not_null` cho `transaction_id`, `subscriber_id`, `msisdn`, `amount`, `status`, `event_time` (row-level); `province_code_completeness` ≥ 90% (warn) |
| `uniqueness` | Có bị ghi trùng không? | `transaction_id_unique`; `business_duplicate`: (`subscriber_id`, `msisdn`, `amount`, `event_time`) không trùng (warn) |
| `validity` | Có đúng định dạng/kiểu/miền giá trị không? | `schema_exact`; `amount_type` decimal, `event_time_type` timestamp; `msisdn_format` `^84\d{9}$` ≥ 97%; `amount_range` [0, 1e8]; `status_domain` |
| `accuracy` | Có phản ánh đúng thực tế không? | `msisdn_carrier_prefix` đầu số di động thực ≥ 99% (warn); `amount_mean` trong [10k, 500k] (warn) |
| `consistency` | Các biểu diễn của cùng một sự kiện có khớp nhau không? | `event_time_in_partition`: `event_time` nằm trong partition `dt` (row-level) |
| `timeliness` | Dữ liệu có về kịp thời không? | `freshness_end_of_day`: `max(event_time)` ≥ `partition_end - 60 phút` (warn) |

### Suite parameter

Rule cần dữ kiện của từng lần chạy dùng `${x}` (trong SQL) hoặc `{"param": "x"}` (trong assert). Job chỉ truyền **dữ kiện**:

```python
dq.validate(df, partition_spec=f"dt={dt}", suite_parameters={
    "partition_start": "2026-10-17 00:00:00",
    "partition_end": "2026-10-18 00:00:00",
    "source_row_count": 10000,     # tính từ Bronze, độc lập với logic transform
})
```

```scala
DQClient.fromSession(spark).validate(df, s"dt=$dt", Map(
  "partition_start" -> s"$dt 00:00:00", "partition_end" -> s"${dt.plusDays(1)} 00:00:00",
  "source_row_count" -> TransactionsTransform.sourceRowCount(raw).toString))
```

## Cấu hình job Spark

| Config | Mặc định | Ý nghĩa |
|---|---|---|
| `spark.jars` | `/opt/dq/jars/dq-platform.jar` | Client DQ. Job Python cần jar này để gọi qua py4j |
| `spark.dq.service.url` | (bắt buộc) | Endpoint DQ service |
| `spark.dq.dataset` | (bắt buộc) | Khóa tra suite trong registry |
| `spark.dq.mode` | `enforce` | `enforce`: FAIL thì không publish, exit khác 0. `monitor`: chỉ ghi kết quả và alert (dùng khi mới bật rule) |
| `spark.dq.fail.on.service.error` | `true` | Service sập (lỗi kết nối, timeout, HTTP 5xx): `true` = fail job (fail-closed, exit 2), `false` = bỏ qua DQ (fail-open, verdict SKIPPED). Lỗi 4xx (sai token, chưa có suite, payload sai) luôn fail job, không bao giờ fail-open |
| `spark.dq.service.timeout.sec` | `10` | Timeout HTTP |
| `spark.dq.quarantine.path` | `<lake>/quarantine` | Nơi ghi dòng lỗi (cột `_dq_errors`, `_dq_run_id`, `_dq_quarantined_at`) |
| `spark.dq.staging.path` | `<lake>/_staging` | Vùng Write của WAP, sau đó rename vào Silver |
| env `DQ_API_TOKEN` | | Token gọi service. Không đặt trong spark.conf vì giá trị sẽ hiện trên Spark UI |

### Hợp đồng `POST /api/v1/runs`

Job gửi kết quả từng rule theo format trung lập, không phụ thuộc Deequ. Service tự đánh giá, nên client không quyết định verdict:

```json
{
  "run_id": "bronze_to_silver_transactions_scala-3f9c1a2b7d10",
  "dataset": "silver.transactions",
  "partition_spec": "dt=2026-10-17",
  "client": "scala",
  "engine": "deequ-2.0.21-spark-3.5",
  "suite_version": 3,
  "parameters": {"partition_start": "2026-10-17 00:00:00", "source_row_count": "10000"},
  "row_count": 10000,
  "quarantined_rows": 308,
  "results": [
    {"rule_id": "province_code_completeness", "kind": "completeness", "dimension": "completeness",
     "severity": "warn", "row_level": false, "threshold": 0.9, "status": "failed",
     "metric_name": "Completeness", "metric_value": 0.7826, "failed_rows": null}
  ]
}
```

`status` là `passed`, `failed` hoặc `error` (rule không chạy được, tính là fail theo severity). Response: `{run_id, verdict, reasons, dimensions}`. Bảng verdict đầy đủ ở [dq-service/README.md](dq-service/README.md).

### Khi lên Spark on Kubernetes

Job Scala (job Python giống bản trước, thêm `spark.jars`):

```bash
spark-submit \
  --master k8s://https://<api-server>:6443 --deploy-mode cluster \
  --name bronze-to-silver-transactions-scala \
  --class dqplatform.jobs.BronzeToSilverTransactions \
  --conf spark.kubernetes.namespace=<data-ns> \
  --conf spark.kubernetes.container.image=<registry>/spark-deequ:3.5.1 \
  --conf spark.kubernetes.driver.secretKeyRef.DQ_API_TOKEN=dq-service-token:token \
  --conf spark.dq.service.url=http://dq-service.<dq-ns>.svc.cluster.local:8000 \
  --conf spark.dq.dataset=silver.transactions \
  --conf spark.dq.mode=enforce \
  --conf spark.dq.fail.on.service.error=true \
  --conf spark.dq.quarantine.path=hdfs://<nameservice>/lake/quarantine \
  --conf spark.dq.staging.path=hdfs://<nameservice>/lake/_staging \
  local:///opt/dq/jars/dq-platform.jar --dt {{ ds }} --lake hdfs://<nameservice>/lake
```

Lưu ý:

- **NetworkPolicy:** chỉ **driver** gọi HTTP tới DQ service, nên chỉ cần mở chiều từ pod driver sang `dq-service`. Executor không gọi service.
- **Deequ chạy hoàn toàn trong JVM:** executor không cần Python package nào. Driver và executor dùng chung image có `dq-platform.jar`.
- **Kerberos/HDFS:** không liên quan đến DQ service, vì Deequ đọc DataFrame đã được Spark load sẵn.
- **Pin version Deequ** (`jvm/pom.xml`). Deequ build riêng cho từng dòng Spark, nên nâng Spark phải chọn bản Deequ có hậu tố khớp. Hành vi Deequ mà client dựa vào được ghim bằng `DeequSemanticsSpec`; nâng version thì chạy `mvn -f jvm/pom.xml test`.

### Build và test

```bash
mvn -f jvm/pom.xml package                                  # test Scala + jvm/dq-jobs/target/dq-platform.jar
pip install -r dq-service/requirements-dev.txt && pytest dq-service/tests
```

### Airflow

Job trả exit code khác 0 khi FAIL, nên task Spark tự fail và chặn các task Gold phía sau, không cần operator riêng:

```python
bronze_to_silver = SparkKubernetesOperator(task_id="bronze_to_silver", ...)
silver_to_gold   = SparkKubernetesOperator(task_id="silver_to_gold", ...)
bronze_to_silver >> silver_to_gold      # DQ FAIL -> silver_to_gold không chạy
```

## Kết quả nghiệm thu

Chạy bằng Spark 3.5.1 local với đúng `dq-platform.jar` và dq-service thật (`scripts/parity_check.py`, dữ liệu mẫu seed 42):

| Kịch bản | Job Python | Job Scala |
|---|---|---|
| Dữ liệu mẫu (`--bad-ratio 0.03`) | exit 0, WARN, 308 quarantine, publish 9.692 | giống hệt |
| `--bad-ratio 0.12` | exit 3, quarantine 12,43%, không publish | giống hệt |
| DQ service sập (fail-closed) | exit 2 | exit 2 |
| Chạy lại cùng `dt` nhiều lần | quarantine vẫn 308 dòng của 1 run | giống hệt |

- Silver và quarantine của hai job giống nhau **từng dòng**, kể cả `_dq_errors`. 21 rule cho cùng status, metric và `failed_rows`. Số liệu khớp baseline của bản Great Expectations trước đó.
- Test: JVM 42, service 40 (pytest).
- **Chưa làm:** build image Docker và `docker compose` thật (daemon không chạy trên máy dev); đo coverage cho Scala.

Code review tìm ra 2 HIGH và 6 MEDIUM, đã sửa hết, mỗi mục có test. Những điều đáng biết khi vận hành:

- **Hai token:** `DQ_API_TOKEN` cho job, `DQ_ADMIN_TOKEN` cho `PUT /suites`. Service không khởi động nếu thiếu. Tách vì rule chứa SQL được chạy trên cluster.
- **Độ phủ rule:** rule của suite mà run không gửi kết quả bị tính fail, nên client lỗi không thể ra PASS bằng cách bỏ sót.
- **Fail-open chỉ cho lỗi mạng, timeout, 5xx.** Lỗi 4xx (sai token, chưa có suite) luôn fail job.
- **Suite parameter** chỉ nhận `[A-Za-z0-9_:.+ -]`, vì giá trị được ghép vào SQL; `'` và `\` đều bị chặn.
- **Publish và quarantine an toàn khi retry:** partition cũ chỉ bị xóa sau khi bản mới đã vào chỗ; quarantine thay nguyên partition `dt`.

## Rủi ro và giới hạn đã biết

| Điều cần lưu ý | Ghi chú |
|---|---|
| Ngữ nghĩa null của Deequ khác Great Expectations | Đã ghim bằng `DeequSemanticsSpec`. Nâng Deequ thì chạy lại `mvn -f jvm/pom.xml test` |
| Deequ build riêng theo dòng Spark | Hiện hỗ trợ tới Spark 3.5.x. Nâng Spark phải chọn bản Deequ có hậu tố khớp |
| py4j không có trên Spark Connect | Job Python hiện chạy `spark-submit` thường. Dùng Spark Connect thì cần xem lại |
| Regex được service kiểm bằng Python `re`, nhưng chạy bằng Java | Service chặn cú pháp `(?P<...>)` của Python; cú pháp chỉ Java có (vd `\p{L}`) bị từ chối. Chỉ xử lý **một phần** |
| Rename atomic chỉ đúng trên HDFS | Trên S3 không atomic; dùng Iceberg (branch / snapshot) |
| Xung đột classpath khi nạp jar vào Spark | Đã exclude `breeze` và `dqdl`; Spark, Scala, Jackson là `provided`. Đã chạy được trên Spark 3.5.1 local, chưa thử trên image thật |

## Đưa lên production cần thêm

- **Rule registry quản lý bằng Git:** rule được review qua PR, CI gọi `PUT /suites`.
- **Dashboard và lineage:** dựng dashboard trên `dq_rule_results` (Grafana/Superset), và đẩy kết quả rule lên DataHub để hiển thị cạnh lineage.
- **Iceberg:** nếu bảng là Iceberg, thay việc rename thư mục bằng branch/snapshot của Iceberg (WAP native).
- **HA cho dq-service:** nhiều replica, Postgres HA, vì service nằm trên đường găng của mọi job khi để fail-closed.
- **Anomaly detection:** dùng anomaly detection của Deequ cho volume và freshness (plan, Phase 7).
