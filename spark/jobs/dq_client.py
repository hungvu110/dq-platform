"""
dq_client — wrapper mỏng cho job Python.

Toàn bộ logic DQ (tải suite, chạy rule bằng Deequ, tách quarantine, gọi DQ service) nằm trong client
JVM jvm/dq-client, nạp qua spark.jars=dq-platform.jar. Job Python và job Scala vì thế dùng chung đúng
một bộ diễn giải rule, không thể cho kết quả khác nhau trên cùng suite.

Cấu hình (spark.conf, client JVM đọc):
  spark.dq.service.url            bắt buộc
  spark.dq.dataset                tên dataset trong registry, vd silver.transactions
  spark.dq.mode                   enforce | monitor           (mặc định enforce)
  spark.dq.fail.on.service.error  true | false                (mặc định true = fail-closed)
  spark.dq.service.timeout.sec    mặc định 10
Token: biến môi trường DQ_API_TOKEN của driver (trên K8s mount từ Secret, không đặt trong spark.conf).
"""

from __future__ import annotations

from dataclasses import dataclass, field

from py4j.protocol import Py4JJavaError
from pyspark.sql import DataFrame, SparkSession

_DQ_FAILED = "dqplatform.client.DQFailedException"


class DQFailed(RuntimeError):
    """DQ service lỗi ở chế độ fail-closed: batch không được kiểm, job nên exit 2."""


@dataclass(frozen=True)
class DQResult:
    run_id: str
    verdict: str  # PASS | WARN | FAIL | SKIPPED
    reasons: list[str]
    row_count: int
    quarantined_rows: int
    valid_df: DataFrame
    quarantine_df: DataFrame | None  # có cột _dq_errors = danh sách rule id bị vi phạm
    suite_version: int | None = None
    _jres: object = field(default=None, repr=False, compare=False)

    @property
    def should_publish(self) -> bool:
        return self.verdict in ("PASS", "WARN", "SKIPPED")

    def release(self) -> None:
        """Gỡ cache client tạo cho batch. Gọi sau khi đã ghi xong valid_df / quarantine_df."""
        if self._jres is not None:
            self._jres.release()


class DQClient:
    def __init__(self, spark: SparkSession):
        self._spark = spark
        jvm_client = spark._jvm.dqplatform.client.DQClient
        try:
            self._j = jvm_client.fromSession(spark._jsparkSession)
        except TypeError as e:  # py4j trả JavaPackage khi không thấy class
            raise RuntimeError(
                "không tìm thấy dqplatform.client.DQClient: thêm dq-platform.jar vào spark.jars / --jars"
            ) from e

    def validate(
        self,
        df: DataFrame,
        partition_spec: str | None = None,
        suite_parameters: dict | None = None,
    ) -> DQResult:
        """suite_parameters: dữ kiện runtime mà suite tham chiếu qua ${x} / {"param": x}.
        Job chỉ truyền dữ kiện (biên partition, số dòng nguồn); ngưỡng nằm trong suite."""
        params = {k: str(v) for k, v in (suite_parameters or {}).items()}
        try:
            jres = self._j.validateForPython(df._jdf, partition_spec, params)
        except Py4JJavaError as e:
            if e.java_exception.getClass().getName() == _DQ_FAILED:
                raise DQFailed(str(e.java_exception.getMessage())) from None
            raise
        jq = jres.quarantineDfOrNull()
        return DQResult(
            run_id=jres.runId(),
            verdict=jres.verdict(),
            reasons=list(jres.reasonsJava()),
            row_count=jres.rowCount(),
            quarantined_rows=jres.quarantinedRows(),
            valid_df=DataFrame(jres.validDf(), self._spark),
            quarantine_df=DataFrame(jq, self._spark) if jq is not None else None,
            suite_version=jres.suiteVersionOrNull(),
            _jres=jres,
        )
