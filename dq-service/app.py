"""
DQ Service — control plane cho data quality.

Không chạm vào dữ liệu. Việc tính toán chạy trong Spark (data plane, "compute to data") bằng Deequ
qua thư viện jvm/dq-client. Service chỉ làm 4 việc:
  1. Rule registry   : lưu suite (rule spec v2, models.py) + policy cho từng dataset, có version
  2. Policy engine   : nhận kết quả từng rule từ job, quyết định PASS / WARN / FAIL (policy.py)
  3. Result store    : lưu lịch sử run + kết quả từng rule -> trend, scorecard 6 dimension (store.py)
  4. Alerting        : bắn webhook khi WARN/FAIL

Hai token:
  DQ_API_TOKEN    job Spark dùng: đọc suite, gửi run, xem kết quả
  DQ_ADMIN_TOKEN  sửa rule (PUT /suites). Tách riêng vì sql_predicate / sql_metric trong suite
                  được chạy như Spark SQL trên cluster -> ai sửa được rule là chạy được SQL.
"""
from __future__ import annotations

import hmac
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path

import requests
from fastapi import BackgroundTasks, Depends, FastAPI, Header, HTTPException
from pydantic import ValidationError
from sqlalchemy.exc import IntegrityError

from models import Policy, RunIn, Suite, SuiteFile, SuiteIn
from policy import decide, dimension_summary, success_percent
from store import Store

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("dq-service")


def create_app(store: Store, api_token: str, admin_token: str, seed_dir: str = "",
               alert_webhook_url: str = "") -> FastAPI:
    if not api_token or not admin_token:
        raise ValueError("DQ_API_TOKEN và DQ_ADMIN_TOKEN bắt buộc, không được rỗng")

    @asynccontextmanager
    async def lifespan(_: FastAPI):
        _seed(store, seed_dir)
        yield

    app = FastAPI(title="DQ Service", version="0.2", lifespan=lifespan)

    def _token_in(authorization: str, *allowed: str) -> bool:
        presented = authorization.removeprefix("Bearer ").encode()
        return authorization.startswith("Bearer ") and any(
            hmac.compare_digest(presented, t.encode()) for t in allowed)

    def auth(authorization: str = Header(default="")) -> None:
        if not _token_in(authorization, api_token, admin_token):
            raise HTTPException(status_code=401, detail="invalid token")

    def admin(authorization: str = Header(default="")) -> None:
        if not _token_in(authorization, admin_token):
            raise HTTPException(status_code=403, detail="cần DQ_ADMIN_TOKEN để sửa rule")

    @app.get("/health")
    def health():
        return {"status": "ok"}

    # ---------- 1. Rule registry ----------
    @app.get("/api/v1/suites/{dataset}", dependencies=[Depends(auth)])
    def get_suite(dataset: str):
        row = store.get_suite(dataset)
        if not row:
            raise HTTPException(404, f"no suite for dataset {dataset}")
        return {"dataset": row["dataset"], "version": row["version"], "suite": row["suite"], "policy": row["policy"]}

    @app.put("/api/v1/suites/{dataset}", dependencies=[Depends(admin)])
    def put_suite(dataset: str, body: SuiteIn):
        # body đã được Pydantic kiểm (kind, dimension, parameter, regex...) -> sai thì 422, không lọt xuống job
        return {"dataset": dataset, "version": store.put_suite(dataset, body.suite, body.policy)}

    # ---------- 2. Policy engine + 3. Result store ----------
    @app.post("/api/v1/runs", dependencies=[Depends(auth)])
    def post_run(run: RunIn, background: BackgroundTasks):
        row = store.get_suite(run.dataset)
        if row:
            policy = Policy.model_validate(row["policy"])
            verdict, reasons = decide(policy, run, Suite.model_validate(row["suite"]).rules, row["version"])
        else:
            policy = Policy()
            verdict, reasons = decide(policy, run)
        try:
            store.save_run(run, verdict, reasons, success_percent(run.results))
        except IntegrityError:
            raise HTTPException(409, f"run_id {run.run_id} đã tồn tại")
        if verdict != "PASS":
            # sau khi trả response: webhook chậm không làm job timeout và fail-closed oan
            background.add_task(_alert, alert_webhook_url, run, verdict, reasons, policy)
        log.info("run %s dataset=%s client=%s verdict=%s reasons=%s",
                 run.run_id, run.dataset, run.client, verdict, reasons)
        return {"run_id": run.run_id, "verdict": verdict, "reasons": reasons,
                "dimensions": dimension_summary(run.results)}

    @app.get("/api/v1/runs", dependencies=[Depends(auth)])
    def list_runs(dataset: str | None = None, limit: int = 20):
        return store.list_runs(dataset, min(max(limit, 1), 500))

    @app.get("/api/v1/runs/{run_id}/rules", dependencies=[Depends(auth)])
    def run_rules(run_id: str):
        return store.rule_results_of(run_id)

    return app


def _seed(store: Store, seed_dir: str) -> None:
    if not seed_dir or not Path(seed_dir).is_dir():
        return
    for f in sorted(Path(seed_dir).glob("*.json")):
        try:
            body = SuiteFile.model_validate_json(f.read_text())
        except ValidationError as e:
            log.error("skip seeding %s: %s", f.name, e)
            continue
        if store.seed_suite(body.dataset, body.suite, body.policy):
            log.info("seeded suite for %s from %s", body.dataset, f.name)


# ---------- 4. Alerting ----------
def _alert(webhook_url: str, run: RunIn, verdict: str, reasons: list[str], policy: Policy) -> None:
    text = (f"DQ {verdict}: {run.dataset} {run.partition_spec or ''} (run {run.run_id}, client {run.client})\n"
            f"owner: {policy.owner}\n" + "\n".join(f"- {r}" for r in reasons))
    if not webhook_url:
        log.warning("ALERT\n%s", text)
        return
    try:
        requests.post(webhook_url, json={"text": text}, timeout=5)
    except requests.RequestException as e:
        log.error("alert webhook failed: %s", e)


app = create_app(
    store=Store(os.getenv("DATABASE_URL", "sqlite:///./dq.db")),
    api_token=os.getenv("DQ_API_TOKEN", ""),
    admin_token=os.getenv("DQ_ADMIN_TOKEN", ""),
    seed_dir=os.getenv("SEED_DIR", ""),
    alert_webhook_url=os.getenv("ALERT_WEBHOOK_URL", ""),
)
