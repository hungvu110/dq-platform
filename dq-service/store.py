"""Rule registry + result store. Postgres dùng schema từ postgres/init.sql; sqlite (dev/test) tự tạo bảng."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from sqlalchemy import (JSON, BigInteger, Boolean, Column, DateTime, Float, Integer, MetaData, String, Table,
                        create_engine, insert, select, update)

from models import Policy, RunIn, Suite

md = MetaData()


def _now() -> datetime:
    return datetime.now(timezone.utc)


suites = Table(
    "dq_suites", md,
    Column("dataset", String, primary_key=True),
    Column("version", Integer, nullable=False, default=1),
    Column("suite", JSON, nullable=False),
    Column("policy", JSON, nullable=False),
    Column("updated_at", DateTime(timezone=True), default=_now),
)
runs = Table(
    "dq_runs", md,
    Column("run_id", String, primary_key=True),
    Column("dataset", String, nullable=False),
    Column("partition_spec", String),
    Column("job_name", String),
    Column("client", String),
    Column("engine", String),
    Column("suite_version", Integer),
    Column("verdict", String, nullable=False),
    Column("reasons", JSON),
    Column("row_count", BigInteger),
    Column("quarantined_rows", BigInteger),
    Column("success_percent", Float),
    Column("raw_result", JSON),
    Column("created_at", DateTime(timezone=True), default=_now),
)
rule_results = Table(
    "dq_rule_results", md,
    Column("id", BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True),
    Column("run_id", String),
    Column("dataset", String),
    Column("rule_id", String),
    Column("kind", String),
    Column("column_name", String),
    Column("dimension", String),
    Column("severity", String),
    Column("row_level", Boolean),
    Column("status", String),
    Column("metric_name", String),
    Column("metric_value", Float),
    Column("failed_rows", BigInteger),
    Column("message", String),
    Column("created_at", DateTime(timezone=True), default=_now),
)


def _dump_suite(suite: Suite) -> dict[str, Any]:
    # by_alias: lưu "assert" (không phải assert_); exclude_none: client coi field vắng = không có
    return suite.model_dump(mode="json", by_alias=True, exclude_none=True)


class Store:
    def __init__(self, url: str):
        self.engine = create_engine(url, pool_pre_ping=True)
        md.create_all(self.engine)  # Postgres: bảng đã có từ init.sql -> no-op

    # ---------- registry ----------
    def get_suite(self, dataset: str) -> dict[str, Any] | None:
        with self.engine.connect() as c:
            row = c.execute(select(suites).where(suites.c.dataset == dataset)).mappings().first()
        return dict(row) if row else None

    def put_suite(self, dataset: str, suite: Suite, policy: Policy) -> int:
        """Tạo mới (version 1) hoặc tăng version."""
        values = {"suite": _dump_suite(suite), "policy": policy.model_dump(mode="json")}
        with self.engine.begin() as c:
            cur = c.execute(select(suites.c.version).where(suites.c.dataset == dataset)).first()
            if cur:
                version = cur[0] + 1
                c.execute(update(suites).where(suites.c.dataset == dataset)
                          .values(version=version, updated_at=_now(), **values))
            else:
                version = 1
                c.execute(insert(suites).values(dataset=dataset, version=version, **values))
        return version

    def seed_suite(self, dataset: str, suite: Suite, policy: Policy) -> bool:
        """Chỉ seed khi dataset chưa có trong registry (không ghi đè rule đã sửa qua API)."""
        if self.get_suite(dataset):
            return False
        self.put_suite(dataset, suite, policy)
        return True

    # ---------- result store ----------
    def save_run(self, run: RunIn, verdict: str, reasons: list[str], success_pct: float | None) -> None:
        with self.engine.begin() as c:
            c.execute(insert(runs).values(
                run_id=run.run_id, dataset=run.dataset, partition_spec=run.partition_spec, job_name=run.job_name,
                client=run.client, engine=run.engine, suite_version=run.suite_version, verdict=verdict,
                reasons=reasons, row_count=run.row_count, quarantined_rows=run.quarantined_rows,
                success_percent=success_pct, raw_result=run.model_dump(mode="json")))
            rows = [dict(
                run_id=run.run_id, dataset=run.dataset, rule_id=r.rule_id, kind=r.kind, column_name=r.column,
                dimension=r.dimension, severity=r.severity, row_level=r.row_level, status=r.status,
                metric_name=r.metric_name, metric_value=r.metric_value, failed_rows=r.failed_rows,
                message=r.message) for r in run.results]
            if rows:
                c.execute(insert(rule_results), rows)

    def list_runs(self, dataset: str | None, limit: int) -> list[dict[str, Any]]:
        q = select(runs.c.run_id, runs.c.dataset, runs.c.partition_spec, runs.c.client, runs.c.engine,
                   runs.c.verdict, runs.c.reasons, runs.c.row_count, runs.c.quarantined_rows,
                   runs.c.success_percent, runs.c.created_at)
        if dataset:
            q = q.where(runs.c.dataset == dataset)
        with self.engine.connect() as c:
            return [dict(r) for r in c.execute(q.order_by(runs.c.created_at.desc()).limit(limit)).mappings()]

    def rule_results_of(self, run_id: str) -> list[dict[str, Any]]:
        with self.engine.connect() as c:
            q = select(rule_results).where(rule_results.c.run_id == run_id).order_by(rule_results.c.id)
            return [dict(r) for r in c.execute(q).mappings()]
