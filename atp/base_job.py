import hashlib
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

    def save_json(self, data: dict | list, path: Path) -> Path:
        """
        Save JSON data to file, creating parent directories as needed.

        :param data: data to serialize
        :param path: absolute path to write JSON file
        :return: path to saved file
        """
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with tmp_path.open("w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        logger.info("Saved JSON to %s", path.relative_to(DATA_ROOT))

        return path

    def read_json(self, path: Path) -> dict | list:
        """
        Read JSON data from file.

        :param path: absolute path to JSON file
        :return: parsed JSON data
        """
        with path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info("Read JSON from %s", path.relative_to(DATA_ROOT))

        return data

    def read_html(self, path: Path) -> str:
        """
        Read HTML content from file.

        :param path: absolute path to HTML file
        :return: HTML string
        """
        with path.open("r", encoding="utf-8") as f:
            content = f.read()

        logger.info("Read HTML from %s", path.relative_to(DATA_ROOT))

        return content

    def save_parquet(self, df: pl.DataFrame, path: Path) -> Path:
        """
        Save polars DataFrame to parquet file, creating parent directories as needed.

        Embeds a schema_hash in parquet metadata for future schema drift detection.

        :param df: DataFrame to save
        :param path: absolute path to write parquet file
        :return: path to saved file
        """
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)

        schema_str = json.dumps([(col, str(dtype)) for col, dtype in df.schema.items()])
        schema_hash = hashlib.md5(schema_str.encode()).hexdigest()[:16]

        try:
            df.write_parquet(
                tmp_path, pyarrow_options={"metadata": {"schema_hash": schema_hash}}
            )
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        logger.info("Saved parquet to %s", path.relative_to(DATA_ROOT))

        return path

    def save_html(self, content: str, path: Path) -> Path:
        """
        Save HTML content to file, creating parent directories as needed.

        :param content: HTML string to write
        :param path: absolute path to write HTML file
        :return: path to saved file
        """
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)

        try:
            with tmp_path.open("w", encoding="utf-8") as f:
                f.write(content)
            tmp_path.replace(path)
        except Exception:
            if tmp_path.exists():
                tmp_path.unlink()
            raise

        logger.info("Saved HTML to %s", path.relative_to(DATA_ROOT))

        return path

    def list_files(self, directory: Path, pattern: str = "*") -> list[Path]:
        """List files matching a glob pattern within a data directory.

        Returns paths sorted by name (ascending).

        :param directory: absolute path to directory to search
        :param pattern: glob pattern to match (default: "*")
        """
        if not directory.is_dir():
            return []
        return sorted(directory.glob(pattern))
