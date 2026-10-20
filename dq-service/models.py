"""
Rule spec v2 (suite) và payload của run, không phụ thuộc engine validate.

Định dạng đầy đủ: docs/DEEQU_MIGRATION_PLAN.md mục 5 và 8.
Client JVM (jvm/dq-client) diễn giải cùng định dạng này -> hai bên phải đổi cùng nhau.
"""
from __future__ import annotations

import re
from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

FORMAT = "dq-rules/v2"
DIMENSIONS = ("completeness", "uniqueness", "validity", "accuracy", "consistency", "timeliness")
Dimension = Literal["completeness", "uniqueness", "validity", "accuracy", "consistency", "timeliness"]
Severity = Literal["error", "warn"]

# kind có kết quả theo dòng -> được phép row_level=true
ROW_LEVEL_KINDS = frozenset({"complete", "completeness", "unique", "pattern", "range", "in_set", "sql_predicate"})
# parameter dành riêng: client tự điền tên temp view của batch (dùng trong sql_metric)
BATCH_PARAM = "batch"
_PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", populate_by_name=True)


# ---------- assertion ----------
class ParamRef(_Model):
    param: str


# NaN/Infinity: JSON của Python chấp nhận, nhưng khi lưu bị thành null -> suite hỏng lúc job chạy
FiniteFloat = Annotated[float, Field(allow_inf_nan=False)]
Bound = Union[FiniteFloat, ParamRef]


class Assertion(_Model):
    op: Literal[">=", ">", "<=", "<", "==", "between"]
    value: Bound | None = None
    min: Bound | None = None
    max: Bound | None = None

    @model_validator(mode="after")
    def _shape(self) -> "Assertion":
        if self.op == "between":
            if self.min is None or self.max is None:
                raise ValueError("op 'between' cần 'min' và 'max'")
        elif self.value is None:
            raise ValueError(f"op '{self.op}' cần 'value'")
        return self

    def param_refs(self) -> set[str]:
        return {b.param for b in (self.value, self.min, self.max) if isinstance(b, ParamRef)}


# ---------- check theo kind (bảng 5.2) ----------
class SizeCheck(_Model):
    kind: Literal["size"]
    assert_: Assertion = Field(alias="assert")


class CompleteCheck(_Model):
    kind: Literal["complete"]
    column: str


class CompletenessCheck(_Model):
    kind: Literal["completeness"]
    column: str
    assert_: Assertion = Field(alias="assert")


class UniqueCheck(_Model):
    kind: Literal["unique"]
    columns: list[str] = Field(min_length=1)
    assert_: Assertion | None = Field(default=None, alias="assert")


class PatternCheck(_Model):
    kind: Literal["pattern"]
    column: str
    regex: str
    assert_: Assertion | None = Field(default=None, alias="assert")

    @field_validator("regex")
    @classmethod
    def _compiles(cls, v: str) -> str:
        # regex do Java thực thi; Python re chỉ là kiểm tra gần đúng cú pháp.
        # Chặn cú pháp chỉ Python có: Java không compile được -> mọi job của dataset sẽ fail.
        if "(?P" in v:
            raise ValueError("named group dạng (?P<name>...) là cú pháp Python; Java dùng (?<name>...)")
        try:
            re.compile(v)
        except re.error as e:
            raise ValueError(f"regex không hợp lệ: {e}") from e
        return v


class RangeCheck(_Model):
    kind: Literal["range"]
    column: str
    min: FiniteFloat
    max: FiniteFloat
    include_min: bool = True
    include_max: bool = True

    @model_validator(mode="after")
    def _ordered(self) -> "RangeCheck":
        if self.min > self.max:
            raise ValueError("range: min > max")
        return self


class InSetCheck(_Model):
    kind: Literal["in_set"]
    column: str
    values: list[str] = Field(min_length=1)


class MeanCheck(_Model):
    kind: Literal["mean"]
    column: str
    assert_: Assertion = Field(alias="assert")


class SqlPredicateCheck(_Model):
    kind: Literal["sql_predicate"]
    predicate: str
    columns: list[str] = Field(min_length=1)
    assert_: Assertion | None = Field(default=None, alias="assert")


class SqlMetricCheck(_Model):
    kind: Literal["sql_metric"]
    sql: str
    assert_: Assertion = Field(alias="assert")

    @field_validator("sql")
    @classmethod
    def _reads_batch(cls, v: str) -> str:
        if "${" + BATCH_PARAM + "}" not in v:
            raise ValueError("sql_metric phải đọc từ ${batch} (temp view của batch đang validate)")
        return v


class SchemaColumnsCheck(_Model):
    kind: Literal["schema_columns"]
    columns: list[str] = Field(min_length=1)
    exact: bool = False


class SchemaTypeCheck(_Model):
    kind: Literal["schema_type"]
    column: str
    type: Literal["string", "boolean", "int", "long", "float", "double", "decimal", "date", "timestamp"]


Check = Annotated[
    Union[SizeCheck, CompleteCheck, CompletenessCheck, UniqueCheck, PatternCheck, RangeCheck, InSetCheck,
          MeanCheck, SqlPredicateCheck, SqlMetricCheck, SchemaColumnsCheck, SchemaTypeCheck],
    Field(discriminator="kind"),
]


def _check_param_refs(check: BaseModel) -> set[str]:
    refs: set[str] = set()
    a = getattr(check, "assert_", None)
    if a is not None:
        refs |= a.param_refs()
    for text in (getattr(check, "predicate", None), getattr(check, "sql", None)):
        if text:
            refs |= set(_PLACEHOLDER.findall(text))
    return refs


# ---------- suite ----------
class Rule(_Model):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]*$")
    dimension: Dimension
    severity: Severity
    row_level: bool = False
    description: str | None = None
    check: Check

    @model_validator(mode="after")
    def _row_level_supported(self) -> "Rule":
        if self.row_level and self.check.kind not in ROW_LEVEL_KINDS:
            raise ValueError(f"rule {self.id}: kind '{self.check.kind}' không hỗ trợ row_level")
        return self


class Suite(_Model):
    format: Literal["dq-rules/v2"]
    description: str | None = None
    parameters: dict[str, str] = Field(default_factory=dict)  # tên -> mô tả; job phải truyền đủ
    rules: list[Rule] = Field(min_length=1)

    @model_validator(mode="after")
    def _consistent(self) -> "Suite":
        ids = [r.id for r in self.rules]
        dup = sorted({i for i in ids if ids.count(i) > 1})
        if dup:
            raise ValueError(f"rule id bị trùng: {', '.join(dup)}")
        if BATCH_PARAM in self.parameters:
            raise ValueError(f"'{BATCH_PARAM}' là parameter dành riêng, không khai báo trong suite")
        for r in self.rules:
            undeclared = _check_param_refs(r.check) - set(self.parameters) - {BATCH_PARAM}
            if undeclared:
                raise ValueError(f"rule {r.id} dùng parameter chưa khai báo: {', '.join(sorted(undeclared))}")
        return self


class Policy(_Model):
    max_quarantine_ratio: float = Field(default=0.05, ge=0, le=1)  # > ngưỡng -> FAIL cả batch
    min_row_count: int = Field(default=1, ge=0)
    owner: str | None = None


class SuiteIn(_Model):
    """Body của PUT /api/v1/suites/{dataset}."""
    suite: Suite
    policy: Policy = Field(default_factory=Policy)


class SuiteFile(SuiteIn):
    """File trong suites/*.json (seed khi service khởi động)."""
    dataset: str


# ---------- run ----------
class RuleResult(BaseModel):
    model_config = ConfigDict(extra="ignore")  # client mới hơn gửi thêm field -> không vỡ

    rule_id: str
    kind: str
    column: str | None = None
    dimension: Dimension
    severity: Severity
    row_level: bool = False
    threshold: float | None = None   # ngưỡng tỉ lệ của rule (vd mostly 0.97); None = không phải rule tỉ lệ
    status: Literal["passed", "failed", "error"]  # error = rule không chạy được
    metric_name: str | None = None
    metric_value: float | None = None
    failed_rows: int | None = Field(default=None, ge=0)
    message: str | None = None


class RunIn(BaseModel):
    model_config = ConfigDict(extra="ignore")

    run_id: str
    dataset: str
    partition_spec: str | None = None
    job_name: str | None = None
    client: str | None = None         # python | scala
    engine: str | None = None         # vd deequ-2.0.21-spark-3.5
    suite_version: int | None = None
    parameters: dict[str, str] = Field(default_factory=dict)
    row_count: int = Field(ge=0)
    quarantined_rows: int = Field(default=0, ge=0)
    results: list[RuleResult]

    @model_validator(mode="after")
    def _counts(self) -> "RunIn":
        if self.quarantined_rows > self.row_count:
            raise ValueError("quarantined_rows > row_count")
        return self
