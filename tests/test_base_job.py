import json
import re
from pathlib import Path

import polars as pl
import pytest

from atp.base_job import BaseJob, BUCKETS, DATA_ROOT


class ConcreteJob(BaseJob):
    """Test subclass with DOMAIN set."""

    DOMAIN = "test_domain"


class TestBaseJobInit:

    def test_missing_domain_raises(self):
        with pytest.raises(NotImplementedError, match="must set DOMAIN"):
            BaseJob()

    def test_subclass_with_domain_succeeds(self):
        job = ConcreteJob()
        assert job.DOMAIN == "test_domain"


class TestRunDatetime:

    def test_run_date_str_format(self):
        job = ConcreteJob()
        assert re.fullmatch(r"\d{8}", job.run_date_str)

    def test_run_datetime_str_format(self):
        job = ConcreteJob()
        assert re.fullmatch(r"\d{8}_\d{6}", job.run_datetime_str)

    def test_attributes_are_consistent(self):
        job = ConcreteJob()
        assert job.run_date_str == job.run_datetime.strftime("%Y%m%d")
        assert job.run_datetime_str == job.run_datetime.strftime("%Y%m%d_%H%M%S")


class TestBuildPath:

    def test_directory_path(self):
        job = ConcreteJob()
        path = job._build_path("raw", "tournaments/tour/339_brisbane/2026")
        assert (
            path
            == DATA_ROOT
            / "raw"
            / "test_domain"
            / "tournaments"
            / "tour"
            / "339_brisbane"
            / "2026"
        )

    def test_file_path(self):
        job = ConcreteJob()
        path = job._build_path(
            "raw", "tournaments/tour/339_brisbane/2026", "overview.json"
        )
        assert path.name == "overview.json"
        assert (
            path.parent
            == DATA_ROOT
            / "raw"
            / "test_domain"
            / "tournaments"
            / "tour"
            / "339_brisbane"
            / "2026"
        )

    def test_invalid_bucket_raises(self):
        job = ConcreteJob()
        with pytest.raises(ValueError, match="Invalid bucket 'invalid'"):
            job._build_path("invalid", "some/path")

    def test_all_buckets_accepted(self):
        job = ConcreteJob()
        for bucket in BUCKETS:
            path = job._build_path(bucket, "some/path")
            assert bucket in str(path)

    def test_domain_override(self):
        job = ConcreteJob()
        path = job._build_path("raw", "some/path", domain="other_domain")
        assert "other_domain" in str(path)
        assert "test_domain" not in str(path)

    def test_version_date(self):
        job = ConcreteJob()
        path = job._build_path("raw", "test", "schedule.html", version="date")
        assert path.name == f"schedule_{job.run_date_str}.html"

    def test_version_datetime(self):
        job = ConcreteJob()
        path = job._build_path("raw", "test", "schedule.html", version="datetime")
        assert path.name == f"schedule_{job.run_datetime_str}.html"

    def test_version_none_leaves_filename_unchanged(self):
        job = ConcreteJob()
        path = job._build_path("raw", "test", "schedule.html", version=None)
        assert path.name == "schedule.html"

    def test_version_invalid_raises(self):
        job = ConcreteJob()
        with pytest.raises(ValueError, match="Invalid version"):
            job._build_path("raw", "test", "schedule.html", version="weekly")


class TestSaveJson:

    def test_save_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        data = {"key": "value", "nested": {"foo": "bar"}}
        target = job._build_path("raw", "test/path", "test.json")
        path = job.save_json(data, target)

        assert path.exists()
        with path.open() as f:
            assert json.load(f) == data

    def test_save_creates_parent_dirs(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = job._build_path("raw", "deep/nested/path", "test.json")
        path = job.save_json({"a": 1}, target)
        assert path.exists()
        assert path.parent.exists()

    def test_save_returns_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = job._build_path("raw", "test", "data.json")
        path = job.save_json([], target)
        assert path == tmp_path / "raw" / "test_domain" / "test" / "data.json"

    def test_save_uses_indent(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = job._build_path("raw", "test", "test.json")
        job.save_json({"a": 1}, target)
        content = target.read_text()
        assert "  " in content  # indented


class TestReadJson:

    def test_read_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        data = {"key": "value", "nested": {"foo": "bar"}}
        target = job._build_path("raw", "test/path", "test.json")
        job.save_json(data, target)
        result = job.read_json(target)

        assert result == data

    def test_read_missing_file_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        missing = tmp_path / "raw" / "test_domain" / "nonexistent" / "missing.json"
        with pytest.raises(FileNotFoundError):
            job.read_json(missing)


class TestSaveHtml:

    def test_save_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        html = "<html><body><h1>Hello</h1></body></html>"
        target = job._build_path("raw", "test/path", "page.html")
        path = job.save_html(html, target)

        assert path.exists()
        assert path.read_text(encoding="utf-8") == html

    def test_save_creates_parent_dirs(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = job._build_path("raw", "deep/nested/path", "page.html")
        path = job.save_html("<html></html>", target)
        assert path.exists()
        assert path.parent.exists()

    def test_save_returns_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = job._build_path("raw", "test", "page.html")
        path = job.save_html("<html></html>", target)
        assert path == tmp_path / "raw" / "test_domain" / "test" / "page.html"


class TestReadHtml:

    def test_read_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        html = "<html><body><h1>Hello</h1></body></html>"
        target = job._build_path("raw", "test/path", "page.html")
        job.save_html(html, target)
        result = job.read_html(target)

        assert result == html

    def test_read_missing_file_raises(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        missing = tmp_path / "raw" / "test_domain" / "missing.html"
        with pytest.raises(FileNotFoundError):
            job.read_html(missing)


class TestSaveParquet:

    def test_save_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        target = job._build_path("stage", "test/path", "test.parquet")
        path = job.save_parquet(df, target)

        assert path.exists()

    def test_save_round_trip(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        df = pl.DataFrame({"a": [1, 2], "b": ["x", "y"]})
        target = job._build_path("stage", "test", "data.parquet")
        path = job.save_parquet(df, target)
        result = pl.read_parquet(path)

        assert result.equals(df)

    def test_save_creates_parent_dirs(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        df = pl.DataFrame({"x": [1]})
        target = job._build_path("stage", "deep/nested", "test.parquet")
        path = job.save_parquet(df, target)

        assert path.exists()
        assert path.parent.exists()

    def test_save_returns_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        df = pl.DataFrame({"x": [1]})
        target = job._build_path("stage", "test", "out.parquet")
        path = job.save_parquet(df, target)

        assert path == tmp_path / "stage" / "test_domain" / "test" / "out.parquet"


class TestListFiles:

    def test_returns_matching_files_sorted(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        dir_path = tmp_path / "raw" / "test_domain" / "test" / "schedule"
        dir_path.mkdir(parents=True)
        (dir_path / "schedule_20260102_100000.html").write_text("a")
        (dir_path / "schedule_20260101_090000.html").write_text("b")
        (dir_path / "schedule_20260103_110000.html").write_text("c")

        result = job.list_files("raw", "test/schedule", "schedule_*.html")

        assert len(result) == 3
        assert result[0].name == "schedule_20260101_090000.html"
        assert result[2].name == "schedule_20260103_110000.html"

    def test_empty_list_for_missing_dir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        result = job.list_files("raw", "nonexistent/path")
        assert result == []

    def test_pattern_filtering(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        dir_path = tmp_path / "raw" / "test_domain" / "test"
        dir_path.mkdir(parents=True)
        (dir_path / "data.json").write_text("{}")
        (dir_path / "data.html").write_text("<html>")
        (dir_path / "other.json").write_text("{}")

        result = job.list_files("raw", "test", "*.json")
        assert len(result) == 2
        assert all(p.suffix == ".json" for p in result)


class TestAtomicWriteCleanup:
    """Verify atomic write cleans up .tmp files and preserves existing targets on failure."""

    def test_save_json_preserves_original_on_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = tmp_path / "raw" / "test_domain" / "test" / "data.json"
        target.parent.mkdir(parents=True)
        target.write_text('{"original": true}')

        # Unserializable value forces json.dump to raise partway through
        with pytest.raises(TypeError):
            job.save_json({"key": object()}, target)

        assert json.loads(target.read_text()) == {"original": True}
        assert list(target.parent.glob("*.tmp")) == []

    def test_save_parquet_preserves_original_on_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = tmp_path / "stage" / "test_domain" / "test" / "data.parquet"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"original content")

        def bad_write(self_df, path, **kwargs):
            Path(path).write_bytes(b"partial")
            raise IOError("disk full")

        monkeypatch.setattr(pl.DataFrame, "write_parquet", bad_write)

        df = pl.DataFrame({"a": [1]})
        with pytest.raises(IOError, match="disk full"):
            job.save_parquet(df, target)

        assert target.read_bytes() == b"original content"
        assert list(target.parent.glob("*.tmp")) == []

    def test_save_html_preserves_original_on_failure(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        target = tmp_path / "raw" / "test_domain" / "test" / "page.html"
        target.parent.mkdir(parents=True)
        target.write_text("<html>original</html>")

        # Non-string content forces f.write() to raise TypeError
        with pytest.raises(TypeError):
            job.save_html(123, target)  # type: ignore[arg-type]

        assert target.read_text() == "<html>original</html>"
        assert list(target.parent.glob("*.tmp")) == []
