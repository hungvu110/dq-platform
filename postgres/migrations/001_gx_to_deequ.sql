-- Nâng DB tạo từ bản Great Expectations lên schema Deequ (rule spec v2). Chạy một lần:
--   docker compose exec -T postgres psql -U dq -d dq -v ON_ERROR_STOP=1 < postgres/migrations/001_gx_to_deequ.sql
-- Sau đó restart dq-service để seed lại suite từ suites/*.json.
BEGIN;

-- dq_runs: thêm client/engine, payload gốc không còn là output GX
ALTER TABLE dq_runs ADD COLUMN IF NOT EXISTS client TEXT;
ALTER TABLE dq_runs ADD COLUMN IF NOT EXISTS engine TEXT;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_name = 'dq_runs' AND column_name = 'gx_result') THEN
        ALTER TABLE dq_runs RENAME COLUMN gx_result TO raw_result;
    END IF;
END $$;
ALTER TABLE dq_runs ADD COLUMN IF NOT EXISTS raw_result JSONB;
UPDATE dq_runs SET engine = 'great_expectations-1.24.0' WHERE engine IS NULL;

-- Kết quả theo rule (thay dq_expectation_results; bảng cũ giữ nguyên làm lịch sử, không ghi thêm)
CREATE TABLE IF NOT EXISTS dq_rule_results (
    id            BIGSERIAL PRIMARY KEY,
    run_id        TEXT REFERENCES dq_runs(run_id) ON DELETE CASCADE,
    dataset       TEXT,
    rule_id       TEXT,
    kind          TEXT,
    column_name   TEXT,
    dimension     TEXT,
    severity      TEXT,
    row_level     BOOLEAN,
    status        TEXT,
    metric_name   TEXT,
    metric_value  DOUBLE PRECISION,
    failed_rows   BIGINT,
    message       TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_dq_rule_dim ON dq_rule_results (dataset, dimension, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_rule_id  ON dq_rule_results (dataset, rule_id, created_at DESC);
COMMENT ON TABLE dq_expectation_results IS 'Lịch sử thời GX (chỉ đọc). Kết quả mới ở dq_rule_results.';

-- Suite định dạng GX không dùng được với client Deequ -> xóa để service seed lại bản v2
DELETE FROM dq_suites WHERE suite->>'format' IS DISTINCT FROM 'dq-rules/v2';

COMMIT;
