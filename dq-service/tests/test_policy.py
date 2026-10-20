from conftest import rule_result, run_payload

from models import Policy, Rule, RunIn
from policy import decide, dimension_summary, success_percent

POLICY = Policy(max_quarantine_ratio=0.05, min_row_count=1)


def _decide(*results, **kw):
    return decide(POLICY, RunIn.model_validate(run_payload(*results, **kw)))


def test_all_passed_is_pass():
    assert _decide(rule_result("a"), rule_result("b")) == ("PASS", [])


def test_failed_warn_rule_gives_warn():
    verdict, reasons = _decide(rule_result("a"), rule_result("b", severity="warn", status="failed",
                                                              dimension="completeness"))
    assert verdict == "WARN"
    assert reasons == ["[warn][completeness] b"]


def test_failed_error_rule_gives_fail_even_after_warn():
    verdict, _ = _decide(rule_result("w", severity="warn", status="failed"),
                         rule_result("e", severity="error", status="failed"))
    assert verdict == "FAIL"


def test_row_level_rule_without_tolerance_is_left_to_quarantine_ratio():
    strict = rule_result("not_null", status="failed", row_level=True, threshold=1.0)
    assert _decide(strict, quarantined=30) == ("PASS", [])
    verdict, reasons = _decide(strict, quarantined=60)  # 6% > 5%
    assert verdict == "FAIL"
    assert reasons == ["quarantine ratio 6.00% > 5.00%"]


def test_row_level_rule_with_own_threshold_fails_batch():
    mostly = rule_result("msisdn_format", status="failed", row_level=True, threshold=0.97)
    assert _decide(mostly, quarantined=10) == ("FAIL", ["[error][validity] msisdn_format"])


def test_rule_that_could_not_run_is_never_treated_as_passed():
    broken = rule_result("fresh", severity="warn", status="error", row_level=True, threshold=1.0,
                         dimension="timeliness", message="SQL lỗi")
    verdict, reasons = _decide(broken)
    assert verdict == "WARN"
    assert reasons == ["[warn][timeliness] fresh (không chạy được: SQL lỗi)"]


def _rule(rule_id: str, severity: str, dimension: str = "validity") -> Rule:
    return Rule.model_validate({"id": rule_id, "dimension": dimension, "severity": severity,
                                "check": {"kind": "complete", "column": "c"}})


def test_rule_of_registered_suite_without_result_is_failed_by_its_severity():
    run = RunIn.model_validate(run_payload(rule_result("a")))
    assert decide(POLICY, run, [_rule("a", "error"), _rule("w", "warn")], 1) == \
        ("WARN", ["[warn][validity] w (thiếu kết quả)"])
    assert decide(POLICY, run, [_rule("a", "error"), _rule("e", "error")], 1)[0] == "FAIL"


def test_suite_changed_during_run_is_warned_not_coverage_checked():
    run = RunIn.model_validate(run_payload(rule_result("a")))  # suite_version = 1
    assert decide(POLICY, run, [_rule("a", "error"), _rule("new", "error")], 2) == \
        ("WARN", ["suite_version 1 khác bản đang đăng ký 2"])


def test_min_row_count_from_policy():
    verdict, reasons = decide(Policy(min_row_count=10), RunIn.model_validate(run_payload(row_count=5)))
    assert verdict == "FAIL"
    assert reasons == ["row_count 5 < 10"]


def test_dimension_summary_and_success_percent():
    run = RunIn.model_validate(run_payload(
        rule_result("a", dimension="completeness"),
        rule_result("b", dimension="completeness", status="failed"),
        rule_result("c", dimension="timeliness", status="error")))
    summary = dimension_summary(run.results)
    assert summary["completeness"] == {"evaluated": 2, "passed": 1}
    assert summary["timeliness"] == {"evaluated": 1, "passed": 0}
    assert summary["accuracy"] == {"evaluated": 0, "passed": 0}
    assert len(summary) == 6
    assert success_percent(run.results) == 100 / 3
    assert success_percent([]) is None
