from unittest.mock import MagicMock

from atp.schemas import Circuit
from atp.tournament.results import ResultsExtractor, _CIRCUIT_URL_PREFIX
from atp.tournament.tournament import Tournament


class TestCircuitUrlPrefixSync:

    def test_all_circuits_have_prefix(self):
        """Every Circuit member must have an entry in _CIRCUIT_URL_PREFIX."""
        for circuit in Circuit:
            assert (
                circuit in _CIRCUIT_URL_PREFIX
            ), f"Circuit.{circuit.name} missing from _CIRCUIT_URL_PREFIX"


class TestResultsUrl:

    def test_tour_url(self):
        t = Tournament(
            tournament_id=375,
            year=2026,
            location="Montpellier, France",
            circuit=Circuit.TOUR,
        )
        ext = ResultsExtractor()
        assert ext._results_url(t) == (
            "https://www.atptour.com/en/scores/current/" "montpellier/375/results"
        )

    def test_challenger_url(self):
        t = Tournament(
            tournament_id=7808,
            year=2026,
            location="Bengaluru, India",
            circuit=Circuit.CHALLENGER,
        )
        ext = ResultsExtractor()
        assert ext._results_url(t) == (
            "https://www.atptour.com/en/scores/current-challenger/"
            "bengaluru/7808/results"
        )


class TestResultsExtractorRun:

    def test_run_saves_both_html_files(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        singles_html = "<html><body>Singles</body></html>"
        doubles_html = "<html><body>Doubles</body></html>"
        fetched_urls = []

        def fake_fetch(url):
            fetched_urls.append(url)
            resp = MagicMock()
            resp.text = doubles_html if "matchType=doubles" in url else singles_html
            return resp

        ext = ResultsExtractor()
        monkeypatch.setattr(ext, "_fetch", fake_fetch)

        t = Tournament(
            tournament_id=375,
            year=2026,
            location="Montpellier, France",
            circuit=Circuit.TOUR,
        )
        ext.run(t)

        assert len(fetched_urls) == 2
        assert any("matchType=doubles" in u for u in fetched_urls)

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        doubles_path = tmp_path / "raw" / "atptour" / t.path / "results_doubles.html"
        assert singles_path.exists()
        assert doubles_path.exists()
        assert singles_path.read_text(encoding="utf-8") == singles_html
        assert doubles_path.read_text(encoding="utf-8") == doubles_html

    def test_run_overwrites_existing(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        t = Tournament(
            tournament_id=375,
            year=2026,
            location="Montpellier, France",
            circuit=Circuit.TOUR,
        )

        def fake_fetch(url):
            resp = MagicMock()
            resp.text = "<html>new</html>"
            return resp

        ext = ResultsExtractor()
        monkeypatch.setattr(ext, "_fetch", fake_fetch)

        # Pre-create file with old content
        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text("old", encoding="utf-8")

        ext.run(t)

        assert singles_path.read_text(encoding="utf-8") == "<html>new</html>"
