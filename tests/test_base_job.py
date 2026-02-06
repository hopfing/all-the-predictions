import json

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


class TestBuildPath:

    def test_directory_path(self):
        job = ConcreteJob()
        path = job._build_path("raw", "tournaments/tour/339_brisbane/2026")
        assert path == DATA_ROOT / "raw" / "test_domain" / "tournaments" / "tour" / "339_brisbane" / "2026"

    def test_file_path(self):
        job = ConcreteJob()
        path = job._build_path("raw", "tournaments/tour/339_brisbane/2026", "overview.json")
        assert path.name == "overview.json"
        assert path.parent == DATA_ROOT / "raw" / "test_domain" / "tournaments" / "tour" / "339_brisbane" / "2026"

    def test_invalid_bucket_raises(self):
        job = ConcreteJob()
        with pytest.raises(ValueError, match="Invalid bucket 'invalid'"):
            job._build_path("invalid", "some/path")

    def test_all_buckets_accepted(self):
        job = ConcreteJob()
        for bucket in BUCKETS:
            path = job._build_path(bucket, "some/path")
            assert bucket in str(path)


class TestSaveJson:

    def test_save_creates_file(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        data = {"key": "value", "nested": {"foo": "bar"}}
        path = job.save_json(data, "raw", "test/path", "test.json")

        assert path.exists()
        with path.open() as f:
            assert json.load(f) == data

    def test_save_creates_parent_dirs(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        path = job.save_json({"a": 1}, "raw", "deep/nested/path", "test.json")
        assert path.exists()
        assert path.parent.exists()

    def test_save_returns_path(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        path = job.save_json([], "raw", "test", "data.json")
        assert path == tmp_path / "raw" / "test_domain" / "test" / "data.json"

    def test_save_uses_indent(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        job = ConcreteJob()

        job.save_json({"a": 1}, "raw", "test", "test.json")
        path = tmp_path / "raw" / "test_domain" / "test" / "test.json"
        content = path.read_text()
        assert "  " in content  # indented
