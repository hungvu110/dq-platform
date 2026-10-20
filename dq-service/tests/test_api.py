import copy

import pytest
from conftest import SUITES_DIR, rule_result, run_payload
from fastapi.testclient import TestClient

from app import create_app
from store import Store

AUTH = {"Authorization": "Bearer tok"}
ADMIN = {"Authorization": "Bearer admin-tok"}


@pytest.fixture
def client(tmp_path):
    app = create_app(Store(f"sqlite:///{tmp_path}/dq.db"), api_token="tok", admin_token="admin-tok",
                     seed_dir=str(SUITES_DIR))
    with TestClient(app) as c:  # with -> chạy lifespan (seed suite)
        yield c


def test_health_is_public_but_api_needs_token(client):
    assert client.get("/health").status_code == 200
    assert client.get("/api/v1/suites/silver.transactions").status_code == 401
    assert client.get("/api/v1/suites/silver.transactions", headers={"Authorization": "Bearer x"}).status_code == 401
    assert client.get("/api/v1/suites/silver.transactions", headers={"Authorization": "Bearer "}).status_code == 401
    assert client.get("/api/v1/suites/silver.transactions", headers=ADMIN).status_code == 200


def test_tokens_are_required(tmp_path):
    with pytest.raises(ValueError, match="bắt buộc"):
        create_app(Store(f"sqlite:///{tmp_path}/a.db"), api_token="", admin_token="x")
    with pytest.raises(ValueError, match="bắt buộc"):
        create_app(Store(f"sqlite:///{tmp_path}/b.db"), api_token="x", admin_token="")


def test_editing_rules_needs_admin_token(client, suite_file):
    body = {"suite": suite_file["suite"], "policy": suite_file["policy"]}
    assert client.put("/api/v1/suites/silver.transactions", headers=AUTH, json=body).status_code == 403
    assert client.put("/api/v1/suites/silver.transactions", headers=ADMIN, json=body).status_code == 200


def test_seeded_suite_is_served_in_client_format(client):
    r = client.get("/api/v1/suites/silver.transactions", headers=AUTH)
    assert r.status_code == 200
    body = r.json()
    assert body["version"] == 1
    assert body["suite"]["format"] == "dq-rules/v2"
    reconcile = next(x for x in body["suite"]["rules"] if x["id"] == "row_count_reconcile")
    assert reconcile["check"]["assert"]["value"] == {"param": "source_row_count"}  # alias "assert", không phải assert_
    assert body["policy"] == {"max_quarantine_ratio": 0.05, "min_row_count": 1, "owner": "team-billing"}


def test_put_suite_validates_and_bumps_version(client, suite_file):
    bad = copy.deepcopy(suite_file)
    bad["suite"]["rules"][0]["dimension"] = "distribution"
    r = client.put("/api/v1/suites/silver.transactions", headers=ADMIN,
                   json={"suite": bad["suite"], "policy": bad["policy"]})
    assert r.status_code == 422

    r = client.put("/api/v1/suites/silver.transactions", headers=ADMIN,
                   json={"suite": suite_file["suite"], "policy": suite_file["policy"]})
    assert r.json() == {"dataset": "silver.transactions", "version": 2}


def test_unknown_dataset_is_404(client):
    assert client.get("/api/v1/suites/nope", headers=AUTH).status_code == 404


def _full_run(suite_file: dict, **overrides) -> dict:
    """Kết quả 'passed' cho đủ 21 rule của suite thật (suite_version 1 = bản seed)."""
    results = [rule_result(r["id"], dimension=r["dimension"], severity=r["severity"],
                           row_level=r.get("row_level", False), kind=r["check"]["kind"])
               for r in suite_file["suite"]["rules"]]
    for r in results:
        r.update(overrides.get(r["rule_id"], {}))
    return run_payload(*results, row_count=10000, quarantined=308)


def test_post_run_decides_stores_and_lists(client, suite_file):
    payload = _full_run(suite_file, province_code_completeness={"status": "failed", "threshold": 0.9})
    r = client.post("/api/v1/runs", headers=AUTH, json=payload)
    assert r.status_code == 200
    body = r.json()
    assert body["verdict"] == "WARN"
    assert body["reasons"] == ["[warn][completeness] province_code_completeness"]
    assert body["dimensions"]["completeness"] == {"evaluated": 9, "passed": 8}

    runs = client.get("/api/v1/runs", headers=AUTH, params={"dataset": "silver.transactions"}).json()
    assert [(x["run_id"], x["verdict"], x["client"], x["quarantined_rows"]) for x in runs] == \
        [("run-1", "WARN", "scala", 308)]
    rules = client.get("/api/v1/runs/run-1/rules", headers=AUTH).json()
    assert len(rules) == 21
    assert (rules[0]["rule_id"], rules[0]["dimension"], rules[0]["status"]) == \
        ("row_count_min", "completeness", "passed")


def test_post_run_missing_rule_results_cannot_pass(client, suite_file):
    payload = _full_run(suite_file)
    payload["results"] = [r for r in payload["results"] if r["rule_id"] != "row_count_reconcile"]
    body = client.post("/api/v1/runs", headers=AUTH, json=payload).json()
    assert body["verdict"] == "FAIL"
    assert body["reasons"] == ["[error][completeness] row_count_reconcile (thiếu kết quả)"]

    payload = run_payload(run_id="run-empty", row_count=10000)  # results rỗng
    assert client.post("/api/v1/runs", headers=AUTH, json=payload).json()["verdict"] == "FAIL"


def test_post_run_duplicate_run_id_is_409(client, suite_file):
    assert client.post("/api/v1/runs", headers=AUTH, json=_full_run(suite_file)).status_code == 200
    assert client.post("/api/v1/runs", headers=AUTH, json=_full_run(suite_file)).status_code == 409


def test_post_run_rejects_inconsistent_payload(client):
    bad = run_payload(rule_result("a"), row_count=10, quarantined=11)
    assert client.post("/api/v1/runs", headers=AUTH, json=bad).status_code == 422
    bad = run_payload(rule_result("a", dimension="distribution"))
    assert client.post("/api/v1/runs", headers=AUTH, json=bad).status_code == 422
