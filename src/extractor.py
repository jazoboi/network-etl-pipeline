"""
Multi-format data extractor.

Reads CSV, SAS7BDAT, and API sources into Spark DataFrames
for ingestion into the Bronze layer.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Result of a data extraction operation."""
    source: str
    format: str
    row_count: int
    columns: list[str]
    df: DataFrame


class DataExtractor:
    """Extracts data from multiple source formats.

    Supports CSV, SAS7BDAT, Parquet, and REST API sources
    with schema inference and validation.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    """

    def __init__(self, spark: SparkSession) -> None:
        self._spark = spark

    def extract(self, source_path: str, format: str = "auto") -> ExtractionResult:
        """Extract data from a file or API source.

        Parameters
        ----------
        source_path : str
            File path or API endpoint URL.
        format : str
            File format (auto-detected from extension if "auto").

        Returns
        -------
        ExtractionResult
            Extracted DataFrame with metadata.
        """
        if format == "auto":
            format = self._detect_format(source_path)

        reader_map = {
            "csv": self._read_csv,
            "sas7bdat": self._read_sas,
            "parquet": self._read_parquet,
            "delta": self._read_delta,
        }

        reader = reader_map.get(format)
        if not reader:
            raise ValueError(f"Unsupported format: {format}")

        df = reader(source_path)

        result = ExtractionResult(
            source=source_path,
            format=format,
            row_count=df.count(),
            columns=df.columns,
            df=df,
        )
        logger.info(
            "Extracted %d rows from %s (%s)",
            result.row_count, source_path, format,
        )
        return result

    def _read_csv(self, path: str) -> DataFrame:
        """Read CSV with schema inference and header detection."""
        return (
            self._spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .option("multiLine", "true")
            .csv(path)
        )

    def _read_sas(self, path: str) -> DataFrame:
        """Read SAS7BDAT via pandas bridge."""
        import pandas as pd
        pdf = pd.read_sas(path, format="sas7bdat")
        return self._spark.createDataFrame(pdf)

    def _read_parquet(self, path: str) -> DataFrame:
        return self._spark.read.parquet(path)

    def _read_delta(self, path: str) -> DataFrame:
        return self._spark.read.format("delta").load(path)

    @staticmethod
    def _detect_format(path: str) -> str:
        """Auto-detect file format from extension."""
        suffix = Path(path).suffix.lower().lstrip(".")
        return {"sas7bdat": "sas7bdat", "csv": "csv", "parquet": "parquet"}.get(suffix, "csv")
