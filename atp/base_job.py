import json
import logging
from pathlib import Path

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

    def _build_path(
        self,
        bucket: str,
        relative_path: str,
        filename: str | None = None,
    ) -> Path:
        """
        Build absolute path within the data directory.

        :param bucket: storage tier — raw, stage, or analytics
        :param relative_path: path within domain (e.g., tournament.path or tournament.path + "/schedule")
        :param filename: optional filename to append
        :return: absolute Path
        """
        if bucket not in BUCKETS:
            raise ValueError(
                f"Invalid bucket '{bucket}'. Must be one of: {', '.join(BUCKETS)}"
            )

        path = DATA_ROOT / bucket / self.DOMAIN / relative_path

        if filename is not None:
            path = path / filename

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
