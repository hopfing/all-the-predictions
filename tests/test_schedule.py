from unittest.mock import MagicMock

from atp.schemas import Circuit
from atp.tournament.schedule import ScheduleExtractor, _CIRCUIT_URL_PREFIX
from atp.tournament.tournament import Tournament


class TestCircuitUrlPrefixSync:

    def test_all_circuits_have_prefix(self):
        """Every Circuit member must have an entry in _CIRCUIT_URL_PREFIX."""
        for circuit in Circuit:
            assert (
                circuit in _CIRCUIT_URL_PREFIX
            ), f"Circuit.{circuit.name} missing from _CIRCUIT_URL_PREFIX"


class TestScheduleUrl:

    def test_tour_url(self):
        t = Tournament(
            tournament_id=339,
            year=2026,
            location="Brisbane, Australia",
            circuit=Circuit.TOUR,
        )
        prefix = _CIRCUIT_URL_PREFIX[t.circuit]
        url = (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{t.url_slug}/{t.tournament_id}/daily-schedule"
        )
        assert url == (
            "https://www.atptour.com/en/scores/current/" "brisbane/339/daily-schedule"
        )

    def test_challenger_url(self):
        t = Tournament(
            tournament_id=1234,
            year=2026,
            location="Champaign, USA",
            circuit=Circuit.CHALLENGER,
        )
        prefix = _CIRCUIT_URL_PREFIX[t.circuit]
        url = (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{t.url_slug}/{t.tournament_id}/daily-schedule"
        )
        assert url == (
            "https://www.atptour.com/en/scores/current-challenger/"
            "champaign/1234/daily-schedule"
        )


class TestScheduleExtractorRun:

    def test_run_saves_html(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        fake_response = MagicMock()
        fake_response.text = "<html><body>Schedule</body></html>"

        ext = ScheduleExtractor()
        monkeypatch.setattr(ext, "_fetch", lambda url: fake_response)

        t = Tournament(
            tournament_id=339,
            year=2026,
            location="Brisbane, Australia",
            circuit=Circuit.TOUR,
        )
        path = ext.run(t)

        assert path.exists()
        assert path.read_text(encoding="utf-8") == fake_response.text

    def test_run_filename_has_datetime(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        fake_response = MagicMock()
        fake_response.text = "<html></html>"

        ext = ScheduleExtractor()
        monkeypatch.setattr(ext, "_fetch", lambda url: fake_response)

        t = Tournament(
            tournament_id=339,
            year=2026,
            location="Brisbane, Australia",
            circuit=Circuit.TOUR,
        )
        path = ext.run(t)

        assert path.name == f"schedule_{ext.run_datetime_str}.html"

    def test_run_path_includes_schedule_subdir(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        fake_response = MagicMock()
        fake_response.text = "<html></html>"

        ext = ScheduleExtractor()
        monkeypatch.setattr(ext, "_fetch", lambda url: fake_response)

        t = Tournament(
            tournament_id=339,
            year=2026,
            location="Brisbane, Australia",
            circuit=Circuit.TOUR,
        )
        path = ext.run(t)

        assert "schedule" in path.parts
        assert "raw" in path.parts
