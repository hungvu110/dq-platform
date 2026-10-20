# dq-service (control plane)

FastAPI service giữ **luật** và **kết quả** của data quality. Nó không chạm vào dữ liệu: Deequ chạy trong Spark ([jvm/dq-client](../jvm/dq-client/README.md)), service chỉ nhận kết quả từng rule rồi quyết định PASS / WARN / FAIL.

Bốn việc của service:

1. **Rule registry:** lưu suite (rule spec v2) và policy cho từng dataset, có version.
2. **Policy engine:** kết quả từng rule và policy của dataset ra verdict.
3. **Result store:** lịch sử run và kết quả từng rule, để vẽ trend và scorecard 6 dimension.
4. **Alerting:** bắn webhook khi verdict khác PASS.

## Cấu trúc

| File | Vai trò |
|---|---|
| [app.py](app.py) | Route, xác thực token, seed suite khi khởi động, alert. Hàm `create_app(...)` nhận store và token từ ngoài nên test được mà không cần biến môi trường |
| [models.py](models.py) | Pydantic: rule spec v2 (`Suite`, `Rule`, các `kind`) và payload của run (`RunIn`, `RuleResult`). Là nơi kiểm suite khi `PUT` |
| [policy.py](policy.py) | Hàm thuần `decide()`, `dimension_summary()`, `success_percent()` |
| [store.py](store.py) | Bảng và truy vấn (SQLAlchemy). Postgres dùng schema từ [postgres/init.sql](../postgres/init.sql); sqlite (dev/test) tự tạo bảng |
| [tests/](tests) | pytest |

Định dạng suite: [docs/RULE_SPEC_V2.md](../docs/RULE_SPEC_V2.md). `models.py` và [RuleSpec.scala](../jvm/dq-client/src/main/scala/dqplatform/client/RuleSpec.scala) là hai bên của cùng một định dạng, nên **đổi một bên thì phải đổi bên kia**.

## API

| Method và path | Token | Mô tả |
|---|---|---|
| `GET /health` | không | Health check |
| `GET /api/v1/suites/{dataset}` | api hoặc admin | `{dataset, version, suite, policy}`; 404 nếu chưa có |
| `PUT /api/v1/suites/{dataset}` | **admin** | Body `{suite, policy}`. Tạo mới hoặc tăng version. Suite sai trả 422 kèm lý do |
| `POST /api/v1/runs` | api hoặc admin | Job gửi kết quả. Trả `{run_id, verdict, reasons, dimensions}`. `run_id` trùng trả 409 |
| `GET /api/v1/runs?dataset=&limit=` | api hoặc admin | Lịch sử run, mới nhất trước (tối đa 500) |
| `GET /api/v1/runs/{run_id}/rules` | api hoặc admin | Kết quả từng rule của một run |

Payload của `POST /runs`: xem mục 8 của [DEEQU_MIGRATION_PLAN.md](../docs/DEEQU_MIGRATION_PLAN.md).

## Cấu hình

| Biến môi trường | Mặc định | Ý nghĩa |
|---|---|---|
| `DQ_API_TOKEN` | **bắt buộc** | Token của job Spark: đọc suite, gửi run |
| `DQ_ADMIN_TOKEN` | **bắt buộc** | Token sửa rule. Không cấp cho job |
| `DATABASE_URL` | `sqlite:///./dq.db` | Postgres: `postgresql+psycopg://user:pass@host:5432/db` |
| `SEED_DIR` | rỗng | Thư mục `*.json` (suite + policy) nạp khi khởi động, chỉ cho dataset chưa có |
| `ALERT_WEBHOOK_URL` | rỗng | Slack/Teams/Telegram. Rỗng thì chỉ ghi log |

Thiếu hoặc rỗng một trong hai token thì service **không khởi động**. Tách hai token vì `sql_predicate` và `sql_metric` trong suite được chạy như Spark SQL trên cluster, nên ai sửa được rule là chạy được SQL.

## Verdict được quyết định thế nào

Logic nằm trong [policy.py](policy.py), mỗi nhánh có test:

| Trường hợp | Kết quả |
|---|---|
| Rule fail, `severity: error` | FAIL |
| Rule fail, `severity: warn` | WARN |
| Rule `row_level` không có ngưỡng riêng (`threshold` rỗng hoặc ≥ 1) bị fail | Bỏ qua: dòng lỗi đã vào quarantine, batch do `max_quarantine_ratio` quyết định |
| Rule `row_level` có ngưỡng riêng < 1 (vd 0.97) và fail | Tính như rule thường |
| Rule `status: error` (không chạy được) | Tính là fail theo severity, không bao giờ coi là đạt |
| `quarantined_rows / row_count` > `policy.max_quarantine_ratio` | FAIL |
| `row_count` < `policy.min_row_count` | FAIL |
| Rule của suite đang đăng ký **không có kết quả** trong run | Tính fail theo severity của rule đó (client lỗi không thể ra PASS bằng cách bỏ sót) |
| `suite_version` của run khác bản đang đăng ký | WARN, và bỏ qua kiểm độ phủ (không biết bản cũ có rule nào) |

`dimension_summary()` trả số rule đạt / đã chạy theo 6 dimension. Job in số này ra log.

## Chạy và test

```bash
pip install -r requirements-dev.txt
pytest tests                                    # chạy từ thư mục dq-service

# Chạy local trên sqlite, seed suite mẫu
DQ_API_TOKEN=dev DQ_ADMIN_TOKEN=dev-admin SEED_DIR=../suites uvicorn app:app --port 8000
```

Trong Docker: `docker compose up -d --build postgres dq-service` (xem [README gốc](../README.md)).

## Lưu ý khi sửa

- Thêm `kind` mới: sửa `models.py` (thêm class check vào union `Check`, và vào `ROW_LEVEL_KINDS` nếu có kết quả theo dòng), sau đó sửa phía Scala (xem README của dq-client). Cập nhật [RULE_SPEC_V2.md](../docs/RULE_SPEC_V2.md).
- Suite lưu với alias `assert` (không phải `assert_`) và bỏ field `None`, vì client Scala đọc đúng dạng đó.
- Webhook chạy bằng `BackgroundTasks`, sau khi response đã trả, nên webhook chậm không làm job timeout.
- Bảng `dq_expectation_results` là lịch sử thời Great Expectations, chỉ đọc. Kết quả mới ở `dq_rule_results`.
