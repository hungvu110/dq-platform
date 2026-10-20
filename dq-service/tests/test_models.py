import copy

import pytest
from pydantic import ValidationError

from models import DIMENSIONS, Suite, SuiteFile


def _suite(*rules: dict, parameters: dict | None = None) -> dict:
    return {"format": "dq-rules/v2", "parameters": parameters or {}, "rules": list(rules)}


def _rule(check: dict, **kw) -> dict:
    return {"id": kw.pop("id", "r"), "dimension": kw.pop("dimension", "validity"),
            "severity": kw.pop("severity", "error"), "check": check, **kw}


def test_suite_file_in_repo_is_valid_and_covers_6_dimensions(suite_file):
    body = SuiteFile.model_validate(suite_file)
    assert len(body.suite.rules) == 21
    assert {r.dimension for r in body.suite.rules} == set(DIMENSIONS)
    assert body.policy.max_quarantine_ratio == 0.05


def test_dump_keeps_assert_alias_and_round_trips(suite_file):
    suite = Suite.model_validate(suite_file["suite"])
    dumped = suite.model_dump(mode="json", by_alias=True, exclude_none=True)
    reconcile = next(r for r in dumped["rules"] if r["id"] == "row_count_reconcile")
    assert reconcile["check"]["assert"] == {"op": "==", "value": {"param": "source_row_count"}}
    assert Suite.model_validate(dumped) == suite


@pytest.mark.parametrize("mutate, expected", [
    (lambda s: s["rules"][0].update(dimension="distribution"), "dimension"),
    (lambda s: s["rules"].append(copy.deepcopy(s["rules"][0])), "trùng"),
    (lambda s: s["rules"][0].update(severity="critical"), "severity"),
    (lambda s: s["rules"][0].update(severty="warn"), "severty"),  # typo field -> bị bắt
    (lambda s: s["rules"][0].update(id="Bad-Id"), "id"),
    (lambda s: s["parameters"].update(batch="x"), "dành riêng"),
])
def test_rejects_invalid_suite(suite_file, mutate, expected):
    suite = copy.deepcopy(suite_file["suite"])
    mutate(suite)
    with pytest.raises(ValidationError, match=expected):
        Suite.model_validate(suite)


def test_rejects_row_level_on_batch_level_kind():
    with pytest.raises(ValidationError, match="không hỗ trợ row_level"):
        Suite.model_validate(_suite(_rule({"kind": "mean", "column": "a", "assert": {"op": ">=", "value": 1}},
                                          row_level=True)))


def test_rejects_undeclared_parameter_in_assert_and_sql():
    with pytest.raises(ValidationError, match="chưa khai báo: n"):
        Suite.model_validate(_suite(_rule({"kind": "size", "assert": {"op": "==", "value": {"param": "n"}}})))
    with pytest.raises(ValidationError, match="chưa khai báo: start"):
        Suite.model_validate(_suite(_rule({"kind": "sql_predicate", "columns": ["t"],
                                           "predicate": "t >= TIMESTAMP '${start}'"})))


def test_sql_metric_must_read_batch_view():
    with pytest.raises(ValidationError, match=r"\$\{batch\}"):
        Suite.model_validate(_suite(_rule({"kind": "sql_metric", "sql": "SELECT 1 FROM t",
                                           "assert": {"op": "==", "value": 1}})))


@pytest.mark.parametrize("check, expected", [
    ({"kind": "pattern", "column": "a", "regex": "(abc"}, "regex không hợp lệ"),
    ({"kind": "pattern", "column": "a", "regex": "(?P<x>a)"}, "cú pháp Python"),
    ({"kind": "size", "assert": {"op": ">=", "value": float("nan")}}, "finite"),
    ({"kind": "range", "column": "a", "min": 0, "max": float("inf")}, "finite"),
    ({"kind": "size", "assert": {"op": ">="}}, "cần 'value'"),
    ({"kind": "mean", "column": "a", "assert": {"op": "between", "min": 1}}, "cần 'min' và 'max'"),
    ({"kind": "range", "column": "a", "min": 5, "max": 1}, "min > max"),
    ({"kind": "schema_type", "column": "a", "type": "varchar"}, "type"),
    ({"kind": "unknown"}, "kind"),
])
def test_rejects_invalid_check(check, expected):
    with pytest.raises(ValidationError, match=expected):
        Suite.model_validate(_suite(_rule(check)))
