import json
import logging
from datetime import datetime
from pathlib import Path

import polars as pl

logger = logging.getLogger(__name__)

DATA_ROOT = Path(__file__).parent.parent / "data"
BUCKETS = ("raw", "stage", "analytics")


class BaseJob:
    """
    Base class providing file I/O and path management for pipeline jobs.

    Subclasses must set DOMAIN to identify their data source (e.g., "atptour").
    """

    DOMAIN: str | None = None

    def __init__(self):
        if self.DOMAIN is None:
            raise NotImplementedError(
                f"{self.__class__.__name__} must set DOMAIN class variable"
            )
        self.run_datetime = datetime.now()
        self.run_date_str = self.run_datetime.strftime("%Y%m%d")
        self.run_datetime_str = self.run_datetime.strftime("%Y%m%d_%H%M%S")

    def _build_path(
        self,
        bucket: str,
        relative_path: str,
        filename: str | None = None,
        domain: str | None = None,
        version: str | None = None,
    ) -> Path:
        """
        Build absolute path within the data directory.

        :param bucket: storage tier — raw, stage, or analytics
        :param relative_path: path within domain (e.g., tournament.path or tournament.path + "/schedule")
        :param filename: optional filename to append
        :param domain: override self.DOMAIN (for cross-domain reads)
        :param version: timestamp suffix for filename — "date" (YYYYMMDD) or "datetime" (YYYYMMDD_HHMMSS)
        :return: absolute Path
        """
        if bucket not in BUCKETS:
            raise ValueError(
                f"Invalid bucket '{bucket}'. Must be one of: {', '.join(BUCKETS)}"
            )

        path = DATA_ROOT / bucket / (domain or self.DOMAIN) / relative_path

        if filename is not None:
            path = path / filename

        if version == "date":
            path = path.with_stem(f"{path.stem}_{self.run_date_str}")
        elif version == "datetime":
            path = path.with_stem(f"{path.stem}_{self.run_datetime_str}")
        elif version is not None:
            raise ValueError(
                f"Invalid version '{version}'. Must be 'date', 'datetime', or None."
            )

        return path

    def save_json(
        self,
        data: dict | list,
        bucket: str,
        relative_path: str,
        filename: str,
    ) -> Path:
        """
        Save JSON data to file, creating parent directories as needed.

        :param data: data to serialize
        :param bucket: storage tier — raw, stage, or analytics
        :param relative_path: path within domain
        :param filename: filename (should end in .json)
        :return: path to saved file
        """
        path = self._build_path(bucket, relative_path, filename)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info("Saved JSON to %s", path.relative_to(DATA_ROOT))

        return path

    def read_json(
        self,
        bucket: str,
        relative_path: str,
        filename: str,
    ) -> dict | list:
        """
        Read JSON data from file.

        :param bucket: storage tier — raw, stage, or analytics
        :param relative_path: path within domain
        :param filename: filename to read
        :return: parsed JSON data
        """
        path = self._build_path(bucket, relative_path, filename)

        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info("Read JSON from %s", path.relative_to(DATA_ROOT))

        return data

    def save_parquet(
        self,
        df: pl.DataFrame,
        bucket: str,
        relative_path: str,
        filename: str,
    ) -> Path:
        """
        Save polars DataFrame to parquet file, creating parent directories as needed.

        :param df: DataFrame to save
        :param bucket: storage tier — raw, stage, or analytics
        :param relative_path: path within domain
        :param filename: filename (should end in .parquet)
        :return: path to saved file
        """
        path = self._build_path(bucket, relative_path, filename)
        path.parent.mkdir(parents=True, exist_ok=True)

        df.write_parquet(path)

        logger.info("Saved parquet to %s", path.relative_to(DATA_ROOT))

        return path

    def save_html(
        self,
        content: str,
        bucket: str,
        relative_path: str,
        filename: str,
        version: str | None = None,
    ) -> Path:
        """
        Save HTML content to file, creating parent directories as needed.

        :param content: HTML string to write
        :param bucket: storage tier — raw, stage, or analytics
        :param relative_path: path within domain
        :param filename: filename (should end in .html)
        :param version: timestamp suffix — "date" (YYYYMMDD) or "datetime" (YYYYMMDD_HHMMSS)
        :return: path to saved file
        """
        path = self._build_path(bucket, relative_path, filename, version=version)
        path.parent.mkdir(parents=True, exist_ok=True)

        with path.open("w", encoding="utf-8") as f:
            f.write(content)

        logger.info("Saved HTML to %s", path.relative_to(DATA_ROOT))

        return path
