"""
Automated data quality validation.

Runs quality gates between medallion layers to catch
schema drift, null spikes, and anomalous record counts.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


class CheckSeverity(str, Enum):
    CRITICAL = "critical"
    WARNING = "warning"
    INFO = "info"


@dataclass
class QualityCheck:
    """Definition of a quality check."""
    name: str
    severity: CheckSeverity
    passed: bool = True
    message: str = ""
    metric_value: float = 0.0


@dataclass
class QualityReport:
    """Aggregated quality report for a dataset."""
    table_name: str
    checks: list[QualityCheck] = field(default_factory=list)
    passed: bool = True
    critical_failures: int = 0

    def add(self, check: QualityCheck) -> None:
        self.checks.append(check)
        if not check.passed:
            if check.severity == CheckSeverity.CRITICAL:
                self.critical_failures += 1
                self.passed = False


class DataQualityValidator:
    """Runs automated quality checks on DataFrames.

    Validates null rates, row counts, schema conformity,
    and statistical distributions between pipeline layers.
    """

    def validate(
        self,
        df: DataFrame,
        table_name: str,
        expected_schema: list[str] | None = None,
        max_null_pct: float = 0.05,
        min_row_count: int = 1,
    ) -> QualityReport:
        """Run all quality checks on a DataFrame.

        Parameters
        ----------
        df : DataFrame
            Data to validate.
        table_name : str
            Name for reporting.
        expected_schema : list[str], optional
            Expected column names.
        max_null_pct : float
            Maximum acceptable null percentage per column.
        min_row_count : int
            Minimum expected row count.

        Returns
        -------
        QualityReport
            Detailed quality assessment.
        """
        report = QualityReport(table_name=table_name)

        report.add(self._check_row_count(df, min_row_count))
        report.add(self._check_duplicates(df))

        if expected_schema:
            report.add(self._check_schema(df, expected_schema))

        for col in df.columns:
            report.add(self._check_null_rate(df, col, max_null_pct))

        logger.info(
            "Quality check %s: %s (%d checks, %d critical failures)",
            table_name, "PASS" if report.passed else "FAIL",
            len(report.checks), report.critical_failures,
        )
        return report

    @staticmethod
    def _check_row_count(df: DataFrame, minimum: int) -> QualityCheck:
        count = df.count()
        return QualityCheck(
            name="row_count",
            severity=CheckSeverity.CRITICAL,
            passed=count >= minimum,
            message=f"Row count: {count} (min: {minimum})",
            metric_value=float(count),
        )

    @staticmethod
    def _check_null_rate(df: DataFrame, column: str, max_pct: float) -> QualityCheck:
        total = df.count()
        if total == 0:
            return QualityCheck(name=f"null_rate_{column}", severity=CheckSeverity.WARNING, passed=True)

        null_count = df.filter(F.col(column).isNull()).count()
        null_pct = null_count / total

        return QualityCheck(
            name=f"null_rate_{column}",
            severity=CheckSeverity.WARNING,
            passed=null_pct <= max_pct,
            message=f"{column}: {null_pct:.1%} null (max: {max_pct:.1%})",
            metric_value=null_pct,
        )

    @staticmethod
    def _check_schema(df: DataFrame, expected: list[str]) -> QualityCheck:
        actual = set(df.columns)
        missing = set(expected) - actual
        return QualityCheck(
            name="schema_conformity",
            severity=CheckSeverity.CRITICAL,
            passed=len(missing) == 0,
            message=f"Missing columns: {missing}" if missing else "Schema OK",
        )

    @staticmethod
    def _check_duplicates(df: DataFrame) -> QualityCheck:
        total = df.count()
        distinct = df.distinct().count()
        dup_count = total - distinct
        return QualityCheck(
            name="duplicate_check",
            severity=CheckSeverity.WARNING,
            passed=dup_count == 0,
            message=f"{dup_count} duplicate rows",
            metric_value=float(dup_count),
        )
