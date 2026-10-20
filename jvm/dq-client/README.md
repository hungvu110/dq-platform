# dq-client (thư viện DQ cho Spark)

Thư viện Scala mà **mọi job Spark** nhúng vào để kiểm data quality bằng [Deequ](https://github.com/awslabs/deequ). Job Scala gọi trực tiếp; job Python gọi qua py4j bằng [spark/jobs/dq_client.py](../../spark/jobs/dq_client.py). Vì cả hai dùng chung một bộ diễn giải rule, cùng một suite không thể cho kết quả khác nhau giữa hai ngôn ngữ.

Stack: Scala 2.12.18, Spark 3.5.1 (`provided`), Deequ `2.0.21-spark-3.5`, Java 11+.

## Một lần `validate()` làm gì

```scala
val res = DQClient.fromSession(spark).validate(df, "dt=2026-10-17", Map("partition_start" -> "2026-10-17 00:00:00", ...))
```

1. Cache `df` (chỉ khi job chưa cache) và đếm số dòng.
2. `GET /suites/{dataset}`, parse suite (`RuleSpec`), kiểm job đã truyền đủ suite parameter (`Params`).
3. Đăng ký `df` thành temp view tạm cho các rule `sql_metric`.
4. Chạy rule: rule schema trên `df.schema`; các rule còn lại thành Deequ Check, chạy chung một `VerificationSuite`.
5. Tách quarantine từ kết quả row-level của Deequ.
6. `POST /runs` với kết quả từng rule, nhận verdict từ service.
7. Trả `DQResult`: `verdict`, `reasons`, `validDf`, `quarantineDf`, `shouldPublish`.

Chỉ **driver** gọi HTTP. Deequ tính metric phân tán trên executor, dữ liệu không rời cluster.

## Các file

| File | Vai trò |
|---|---|
| [RuleSpec.scala](src/main/scala/dqplatform/client/RuleSpec.scala) | Parse và kiểm suite JSON thành `Suite` / `Rule` / `CheckSpec` |
| [Params.scala](src/main/scala/dqplatform/client/Params.scala) | Thay `${x}` và `{"param": x}`, kiểm giá trị parameter, `ResolvedAssertion` (hàm kiểm + ngưỡng) |
| [DeequChecks.scala](src/main/scala/dqplatform/client/DeequChecks.scala) | Mỗi `Rule` thành một Deequ `Check` (bảng ánh xạ ở mục 5.2 của [plan](../../docs/DEEQU_MIGRATION_PLAN.md)) |
| [SchemaRules.scala](src/main/scala/dqplatform/client/SchemaRules.scala) | Rule `schema_columns` / `schema_type`, chạy trên `df.schema` (Deequ không có) |
| [RowLevel.scala](src/main/scala/dqplatform/client/RowLevel.scala) | `rowLevelResultsAsDataFrame` thành cột `_dq_errors`, tách `valid` / `quarantine`, đếm `failed_rows` từng rule |
| [RuleResult.scala](src/main/scala/dqplatform/client/RuleResult.scala) | Kết quả một rule theo format trung lập gửi lên service |
| [ServiceClient.scala](src/main/scala/dqplatform/client/ServiceClient.scala) | `ServiceApi` (trait, để test không cần HTTP) và `HttpServiceApi` |
| [DQClient.scala](src/main/scala/dqplatform/client/DQClient.scala) | `DQClient`, `DQConfig`, `DQResult`, `DQFailedException` |

## Cấu hình

Đọc từ `spark.conf`, token từ biến môi trường của driver:

| Config | Mặc định | Ý nghĩa |
|---|---|---|
| `spark.dq.service.url` | bắt buộc | Địa chỉ DQ service |
| `spark.dq.dataset` | bắt buộc | Khóa suite trong registry |
| `spark.dq.mode` | `enforce` | `monitor`: verdict FAIL bị hạ xuống WARN, vẫn publish |
| `spark.dq.fail.on.service.error` | `true` | Xem bên dưới |
| `spark.dq.service.timeout.sec` | `10` | Timeout HTTP |
| env `DQ_API_TOKEN` | | Không đặt trong spark.conf vì giá trị sẽ hiện trên Spark UI |

### Khi service lỗi

| Tình huống | `fail.on.service.error=true` | `=false` |
|---|---|---|
| Không kết nối được, timeout, HTTP 5xx | ném `DQFailedException` (job exit 2) | verdict `SKIPPED`, dữ liệu không được kiểm, vẫn publish |
| HTTP 4xx (sai token, chưa có suite, payload bị từ chối) | ném `DQFailedException` | **vẫn ném**: đây là lỗi cấu hình, không phải "service sập" |

## Vòng đời cache

Client chỉ gỡ cache do **chính nó** tạo. Gọi `res.release()` sau khi đã ghi xong `validDf` và `quarantineDf` (cả hai là view lười trên batch đã cache). Nếu `validate` ném lỗi, client tự gỡ.

## Test

```bash
cd jvm && mvn -B -pl dq-client -am test
```

| Spec | Kiểm gì |
|---|---|
| `DeequSemanticsSpec` | **Ghim hành vi của Deequ** mà client dựa vào (null, row-level, `customSql`...). Nâng version Deequ làm đổi hành vi thì spec này đỏ |
| `RuleSpecSpec`, `ParamsSpec` | Parse suite thật trong repo, từ chối suite sai, an toàn của parameter |
| `DQClientSpec` | Suite thật qua `DQClient` với service giả: kết quả từng rule, quarantine theo rule id, fail-open / fail-closed, cache |
| `HttpServiceApiSpec` | HTTP thật tới server giả: header, encode path, phân loại lỗi |

Test đọc `../../suites/silver_transactions.json`, nên chạy từ trong repo.

## Thêm một `kind` rule mới

1. `RuleSpec.scala`: thêm case class vào `CheckSpec` và nhánh parse (thêm vào `RowLevelKinds` nếu có kết quả theo dòng).
2. `DeequChecks.scala`: ánh xạ sang Deequ Check, và `threshold` nếu là rule tỉ lệ.
3. Viết test trong `DeequSemanticsSpec` (hành vi Deequ, gồm null) và `DQClientSpec`.
4. Phía service: thêm model vào [dq-service/models.py](../../dq-service/models.py), và cập nhật [RULE_SPEC_V2.md](../../docs/RULE_SPEC_V2.md).

## Những điều dễ vấp

- **Null:** `hasPattern` của Deequ coi null là không khớp. Client thêm `.where("col IS NOT NULL")` và `FilteredRowOutcome.TRUE` để khớp quy ước "null là việc của rule completeness". `sql_predicate` thì tự viết null-safe.
- **Bỏ temp view:** dùng `sessionState.catalog.dropTempView`. `spark.catalog.dropTempView` gỡ luôn cache của batch.
- **HTTP/1.1:** `HttpServiceApi` ép HTTP/1.1. HttpClient mặc định gửi `Upgrade: h2c` và uvicorn làm mất body của POST.
- **Tên `Suite`:** class `Suite` trong package này trùng `org.scalatest.Suite`; trong test dùng `Suite => TestSuite`.
- **Spark và Scala** là `provided`; Deequ khai báo chúng ở scope compile nên `jvm/pom.xml` exclude hết, cùng `breeze` và `dqdl`.
