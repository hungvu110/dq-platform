# Rule spec v2 (`dq-rules/v2`)

Định dạng suite trong `suites/*.json` và trong rule registry của dq-service. Nó không phụ thuộc engine: client JVM (`jvm/dq-client`) dịch từng rule sang Deequ Check, còn dq-service chỉ kiểm cú pháp và áp policy.

Nguồn sự thật của định dạng là [dq-service/models.py](../dq-service/models.py) (khi `PUT`) và [RuleSpec.scala](../jvm/dq-client/src/main/scala/dqplatform/client/RuleSpec.scala) (khi job chạy). Đổi định dạng thì phải sửa cả hai nơi.

## File suite

```jsonc
{
  "dataset": "silver.transactions",          // khóa trong registry = spark.dq.dataset của job
  "policy": {
    "max_quarantine_ratio": 0.05,            // > 5% số dòng vào quarantine -> FAIL cả batch
    "min_row_count": 1,                      // mặc định 1
    "owner": "team-billing"                  // hiện trong alert
  },
  "suite": {
    "format": "dq-rules/v2",
    "description": "...",
    "parameters": {                          // dữ kiện job phải truyền khi validate (tên -> mô tả)
      "partition_start": "đầu ngày dt"
    },
    "rules": [ /* xem bên dưới */ ]
  }
}
```

Nạp bằng một trong hai cách:

- **Seed:** service tự seed khi khởi động, nhưng chỉ cho dataset chưa có trong registry.
- **API:** `PUT /api/v1/suites/{dataset}` với body `{suite, policy}`. Mỗi lần PUT tăng version. Suite sai bị trả 422 kèm lý do.

## Rule

```jsonc
{
  "id": "msisdn_format",       // [a-z][a-z0-9_]*, duy nhất trong suite; là tên lỗi trong cột _dq_errors
  "dimension": "validity",     // completeness | uniqueness | validity | accuracy | consistency | timeliness
  "severity": "error",         // error: fail -> FAIL batch; warn: fail -> WARN, vẫn publish, có alert
  "row_level": true,           // true: dòng vi phạm vào quarantine (chỉ với kind có kết quả theo dòng)
  "description": "...",        // tùy chọn
  "check": { "kind": "pattern", "column": "msisdn", "regex": "^84[0-9]{9}$",
             "assert": { "op": ">=", "value": 0.97 } }
}
```

### Rule ảnh hưởng tới verdict thế nào

| Trường hợp | Kết quả |
|---|---|
| Rule đạt | Không ảnh hưởng |
| Rule fail, `severity: error` | FAIL |
| Rule fail, `severity: warn` | WARN |
| Rule `row_level` không có ngưỡng riêng (mặc định `== 1.0`) bị fail | **Không** tự làm FAIL. Dòng lỗi đã vào quarantine, và batch do `max_quarantine_ratio` quyết định |
| Rule `row_level` có ngưỡng riêng (vd `>= 0.97`) và tỉ lệ thấp hơn ngưỡng | Rule fail theo severity, dòng lỗi vẫn vào quarantine |
| Rule không chạy được (cột không tồn tại, SQL sai...) | `status: error`, tính là fail theo severity. Không bao giờ bị coi là đạt |

## `assert`

```jsonc
{ "op": ">=", "value": 0.97 }                       // op: >= > <= < ==
{ "op": "between", "min": 10000, "max": 500000 }    // hai đầu đều tính là đạt
{ "op": "==", "value": { "param": "source_row_count" } }   // lấy từ suite parameter
```

Với kind dạng tỉ lệ (`complete`, `unique`, `pattern`, `range`, `in_set`, `sql_predicate`), bỏ `assert` nghĩa là `== 1.0`: mọi dòng phải đạt.

## Các `kind`

| kind | Field | Đo gì | Row-level |
|---|---|---|---|
| `size` | `assert` | Số dòng | không |
| `complete` | `column` | Không null (mọi dòng) | có |
| `completeness` | `column`, `assert` | Tỉ lệ không null | có |
| `unique` | `columns[]`, `assert`? | Tỉ lệ giá trị (tổ hợp cột) xuất hiện đúng 1 lần, bỏ qua dòng null | có |
| `pattern` | `column`, `regex`, `assert`? | Tỉ lệ khớp regex trên các dòng **không null** | có |
| `range` | `column`, `min`, `max`, `include_min`?, `include_max`? | Giá trị trong khoảng; null được tính là đạt | có |
| `in_set` | `column`, `values[]` | Giá trị thuộc tập; null được tính là đạt | có |
| `mean` | `column`, `assert` | Trung bình | không |
| `sql_predicate` | `predicate`, `columns[]`, `assert`? | Tỉ lệ dòng thỏa điều kiện SQL | có |
| `sql_metric` | `sql`, `assert` | Một số do câu SQL trả về, đọc từ `${batch}` | không |
| `schema_columns` | `columns[]`, `exact`? | Tập cột (exact: không thừa không thiếu) | không |
| `schema_type` | `column`, `type` | Kiểu Spark: `string`, `boolean`, `int`, `long`, `float`, `double`, `decimal`, `date`, `timestamp` | không |

### Null: mỗi rule chỉ đo một việc

Null là việc của rule completeness (`complete` / `completeness`). Các kind khác bỏ qua null, để một dòng thiếu dữ liệu không bị tính lỗi hai lần:

- **`pattern`:** client tự thêm điều kiện `col IS NOT NULL`. Nếu không, Deequ sẽ coi null là không khớp.
- **`range`, `in_set`:** Deequ đã tự sinh `col IS NULL OR ...`.
- **`sql_predicate`:** **phải tự viết null-safe**, ví dụ `event_time IS NULL OR (event_time >= ...)`. Predicate trả về null sẽ bị tính là **fail**.

Các hành vi này được ghim bằng test trong [DeequSemanticsSpec.scala](../jvm/dq-client/src/test/scala/dqplatform/client/DeequSemanticsSpec.scala).

## Suite parameter

Job truyền dữ kiện của từng lần chạy, còn ngưỡng nằm trong suite. Nhờ vậy đổi ngưỡng không cần deploy lại job.

```jsonc
"predicate": "event_time IS NULL OR (event_time >= TIMESTAMP '${partition_start}' AND event_time < TIMESTAMP '${partition_end}')"
"sql":       "SELECT CAST(max(event_time) >= TIMESTAMP '${partition_end}' - INTERVAL 60 MINUTES AS DOUBLE) FROM ${batch}"
"assert":    { "op": "==", "value": { "param": "source_row_count" } }
```

- Mọi `${x}` và `{"param": x}` phải được khai báo trong `suite.parameters`. Service từ chối suite tham chiếu parameter chưa khai báo, còn client fail nếu job không truyền đủ.
- `${batch}` là parameter dành riêng: tên temp view của batch đang validate, do client tự điền.
- Giá trị parameter chỉ được gồm chữ, số, khoảng trắng và `_ : . + -`, đủ cho ngày giờ và số. Giá trị được ghép thẳng vào SQL, nên client chặn mọi ký tự khác, kể cả `'` và `\` (dấu `\` có thể escape dấu nháy đóng chuỗi).
- Regex chạy bằng Java (`java.util.regex`). Service chỉ kiểm cú pháp gần đúng bằng Python `re`, và từ chối named group kiểu Python `(?P<name>...)`. Cú pháp chỉ Java có, như `\p{L}`, hiện bị service từ chối; cần thì viết lại bằng lớp ký tự thường.

```python
# Python
dq.validate(df, partition_spec="dt=2026-10-17", suite_parameters={"partition_start": "2026-10-17 00:00:00", ...})
```

```scala
// Scala
DQClient.fromSession(spark).validate(df, "dt=2026-10-17", Map("partition_start" -> "2026-10-17 00:00:00", ...))
```

## Kết quả gửi lên service

Với mỗi rule, client gửi `{rule_id, kind, column, dimension, severity, row_level, threshold, status, metric_name, metric_value, failed_rows, message}`. Service lưu các bản ghi này vào bảng `dq_rule_results`. Chi tiết hợp đồng API ở mục 8 của [DEEQU_MIGRATION_PLAN.md](DEEQU_MIGRATION_PLAN.md).
