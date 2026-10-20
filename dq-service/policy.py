"""Policy engine: kết quả từng rule + policy của dataset -> verdict PASS / WARN / FAIL. Hàm thuần, dễ test."""
from __future__ import annotations

from models import DIMENSIONS, Policy, Rule, RuleResult, RunIn


def _tolerates_rows(r: RuleResult) -> bool:
    """Rule row-level không có ngưỡng riêng (= 1.0): dòng lỗi đã vào quarantine, batch do
    max_quarantine_ratio quyết định. Có ngưỡng < 1.0 (vd mostly 0.97) mà vẫn fail -> rule tự tính."""
    return r.row_level and (r.threshold is None or r.threshold >= 1.0)


def decide(policy: Policy, run: RunIn, suite_rules: list[Rule] | None = None,
           suite_version: int | None = None) -> tuple[str, list[str]]:
    """suite_rules / suite_version: suite đang đăng ký. Rule của suite mà run không gửi kết quả
    bị tính là fail theo severity -> client lỗi hoặc cũ không thể "PASS" bằng cách bỏ sót rule."""
    reasons: list[str] = []
    verdict = "PASS"

    def flag(severity: str, reason: str) -> None:
        nonlocal verdict
        reasons.append(reason)
        if severity == "error":
            verdict = "FAIL"
        elif verdict == "PASS":
            verdict = "WARN"

    for r in run.results:
        if r.status == "passed":
            continue
        if r.status == "failed" and _tolerates_rows(r):
            continue
        reason = f"[{r.severity}][{r.dimension}] {r.rule_id}"
        if r.status == "error":  # rule không chạy được thì không được coi là đạt
            reason += f" (không chạy được: {r.message})"
        flag(r.severity, reason)

    if suite_rules is not None:
        if run.suite_version != suite_version:
            # suite đổi giữa lúc job chạy: không biết bản cũ có những rule nào -> không kiểm độ phủ được
            flag("warn", f"suite_version {run.suite_version} khác bản đang đăng ký {suite_version}")
        else:
            reported = {r.rule_id for r in run.results}
            for rule in suite_rules:
                if rule.id not in reported:
                    flag(rule.severity, f"[{rule.severity}][{rule.dimension}] {rule.id} (thiếu kết quả)")

    ratio = run.quarantined_rows / run.row_count if run.row_count else 0.0
    if ratio > policy.max_quarantine_ratio:
        verdict = "FAIL"
        reasons.append(f"quarantine ratio {ratio:.2%} > {policy.max_quarantine_ratio:.2%}")
    if run.row_count < policy.min_row_count:
        verdict = "FAIL"
        reasons.append(f"row_count {run.row_count} < {policy.min_row_count}")
    return verdict, reasons


def dimension_summary(results: list[RuleResult]) -> dict[str, dict[str, int]]:
    """Số rule đạt / đã chạy theo từng dimension -> scorecard 6 chiều cho mỗi run."""
    out = {d: {"evaluated": 0, "passed": 0} for d in DIMENSIONS}
    for r in results:
        out[r.dimension]["evaluated"] += 1
        out[r.dimension]["passed"] += r.status == "passed"
    return out


def success_percent(results: list[RuleResult]) -> float | None:
    if not results:
        return None
    return 100.0 * sum(r.status == "passed" for r in results) / len(results)
