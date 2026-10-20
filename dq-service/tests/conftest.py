import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

SERVICE_DIR = Path(__file__).resolve().parents[1]
REPO_DIR = SERVICE_DIR.parent
SUITES_DIR = REPO_DIR / "suites"

sys.path.insert(0, str(SERVICE_DIR))
# app.py tạo app mặc định lúc import -> trỏ DB về thư mục tạm (không tạo dq.db trong repo), có token
os.environ.setdefault("DATABASE_URL", f"sqlite:///{tempfile.mkdtemp()}/import.db")
os.environ.setdefault("DQ_API_TOKEN", "import-api-token")
os.environ.setdefault("DQ_ADMIN_TOKEN", "import-admin-token")


@pytest.fixture
def suite_file() -> dict:
    """suites/silver_transactions.json: {dataset, policy, suite}."""
    return json.loads((SUITES_DIR / "silver_transactions.json").read_text())


def rule_result(rule_id: str = "r", *, dimension: str = "validity", severity: str = "error", status: str = "passed",
                row_level: bool = False, threshold: float | None = None, kind: str = "complete",
                message: str | None = None) -> dict:
    return {"rule_id": rule_id, "kind": kind, "column": "c", "dimension": dimension, "severity": severity,
            "row_level": row_level, "threshold": threshold, "status": status, "metric_name": "Completeness",
            "metric_value": 1.0 if status == "passed" else 0.5, "failed_rows": 0 if row_level else None,
            "message": message}


def run_payload(*results: dict, row_count: int = 1000, quarantined: int = 0, dataset: str = "silver.transactions",
                run_id: str = "run-1") -> dict:
    return {"run_id": run_id, "dataset": dataset, "partition_spec": "dt=2026-10-17", "job_name": "job",
            "client": "scala", "engine": "deequ-2.0.21-spark-3.5", "suite_version": 1,
            "parameters": {"partition_start": "2026-10-17 00:00:00"}, "row_count": row_count,
            "quarantined_rows": quarantined, "results": list(results)}
