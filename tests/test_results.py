from datetime import date
from unittest.mock import MagicMock

import polars as pl
import pytest

from atp.schemas import Circuit, Round
from atp.tournament.results import (
    ResultsExtractor,
    ResultsTransformer,
)
from atp.tournament.tournament import Tournament


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


# --- HTML fixtures for transformer tests ---


def _match_html(
    round_text="Quarterfinals",
    court="Court 1",
    duration="01:30:00",
    winner_id="ab12",
    winner_name="A. Winner",
    winner_seed="(1)",
    loser_id="cd34",
    loser_name="C. Loser",
    loser_seed="",
    winner_scores=((6,), (7, 5)),
    loser_scores=((4,), (6,)),
    notes="Game Set and Match A. Winner.",
    match_code="ms001",
    umpire="John Doe",
    is_doubles=False,
    winner_partner=None,
    loser_partner=None,
):
    """Build a minimal but valid match div HTML string."""
    header_parts = f"<strong>{round_text}"
    if court:
        header_parts += f" - {court}"
    header_parts += "</strong>"

    duration_span = f"<span>{duration}</span>" if duration else ""

    def _side_html(pid, pname, seed_text, scores, is_winner, partner=None):
        winner_div = (
            '<div class="winner"><span class="icon-checkmark"></span></div>'
            if is_winner
            else ""
        )

        if partner:
            p_id, p_name = partner
            names_html = f"""
            <div class="players"><div class="names">
                <div class="name">
                    <a href="/en/players/x/{pid}/overview">{pname}</a>
                    <span>{seed_text}</span>
                </div>
                <div class="name">
                    <a href="/en/players/x/{p_id}/overview">{p_name}</a>
                    <span>{seed_text}</span>
                </div>
            </div></div>
            """
        else:
            names_html = f"""
            <div class="name">
                <a href="/en/players/x/{pid}/overview">{pname}</a>
                <span>{seed_text}</span>
            </div>
            """

        score_items = '<div class="score-item"></div>'  # spacer
        for spans in scores:
            inner = "".join(f"<span>{s}</span>" for s in spans)
            score_items += f'<div class="score-item">{inner}</div>'

        return f"""
        <div class="stats-item">
            <div class="player-info">
                {names_html}
                {winner_div}
            </div>
            <div class="scores">{score_items}</div>
        </div>
        """

    winner_html = _side_html(
        winner_id, winner_name, winner_seed, winner_scores, True, winner_partner
    )
    loser_html = _side_html(
        loser_id, loser_name, loser_seed, loser_scores, False, loser_partner
    )

    stats_link = (
        f'<a href="/en/scores/stats-centre/archive/2026/375/{match_code}">Stats</a>'
        if match_code
        else ""
    )

    return f"""
    <div class="match">
        <div class="match-header">
            <span>{header_parts}</span>
            {duration_span}
        </div>
        <div class="match-content">
            <div class="match-stats">
                {winner_html}
                {loser_html}
            </div>
        </div>
        <div class="match-footer">
            <div class="match-umpire">Ump: {umpire}</div>
            <div class="match-cta">
                <a href="/en/players/atp-head-2-head/x">H2H</a>
                {stats_link}
            </div>
        </div>
        <div class="match-notes">{notes}</div>
    </div>
    """


def _day_html(day_num, date_str, matches_html):
    """Wrap match HTML in a day accordion item."""
    return f"""
    <div class="atp_accordion-item">
        <div class="atp_accordion-header">
            <div class="tournament-day">
                <h4>
                    {date_str}
                    <span>Day ({day_num})</span>
                </h4>
            </div>
        </div>
        <div class="atp_accordion-content">
            <div class="match-group match-group--active">
                <div class="match-group-content">
                    {matches_html}
                </div>
            </div>
        </div>
    </div>
    """


def _results_page(days_html):
    """Wrap day HTML in full page structure."""
    return f"""
    <html><body>
    <div class="atp_accordion">
        <div class="atp_accordion-items">
            {days_html}
        </div>
    </div>
    </body></html>
    """


def _make_tournament():
    return Tournament(
        tournament_id=375,
        year=2026,
        location="Montpellier, France",
        circuit=Circuit.TOUR,
    )


class TestDetermineStatus:

    def test_completed(self):
        assert (
            ResultsTransformer._determine_status("Game Set and Match A. Winner.")
            == "completed"
        )

    def test_retired(self):
        assert (
            ResultsTransformer._determine_status(
                "Game Set and Match A. Winner. A. Winner wins the match 6-3 4-5 RET."
            )
            == "retired"
        )

    def test_walkover(self):
        assert (
            ResultsTransformer._determine_status(
                "Winners: PLAYER A / PLAYER B by Walkover"
            )
            == "walkover"
        )

    def test_in_progress_returns_none(self):
        assert (
            ResultsTransformer._determine_status(
                "A. Winner wins the point on his 1st serve."
            )
            is None
        )


class TestParseDate:

    def test_standard_format(self):
        assert ResultsTransformer._parse_date("Sat, 07 February, 2026") == date(
            2026, 2, 7
        )

    def test_single_digit_day(self):
        assert ResultsTransformer._parse_date("Mon, 2 February, 2026") == date(
            2026, 2, 2
        )


class TestParseDuration:

    def test_normal(self):
        from bs4 import BeautifulSoup

        html = '<div class="match-header"><span><strong>QF</strong></span><span>01:30:45</span></div>'
        header = BeautifulSoup(html, "lxml").find("div", class_="match-header")
        assert ResultsTransformer._parse_duration(header) == 5445

    def test_no_duration_span(self):
        from bs4 import BeautifulSoup

        html = '<div class="match-header"><span><strong>QF</strong></span></div>'
        header = BeautifulSoup(html, "lxml").find("div", class_="match-header")
        assert ResultsTransformer._parse_duration(header) is None


class TestParseSeedEntry:

    @pytest.mark.parametrize(
        "html_span, expected",
        [
            ("<span>(1)</span>", (1, None)),
            ("<span>(WC)</span>", (None, "WC")),
            ("<span>(1/W)</span>", (1, "W")),
            ("<span>(Q)</span>", (None, "Q")),
            ("<span></span>", (None, None)),
        ],
    )
    def test_parse(self, html_span, expected):
        from bs4 import BeautifulSoup

        html = f'<div class="name"><a href="/en/players/x/ab12/overview">Name</a>{html_span}</div>'
        name_div = BeautifulSoup(html, "lxml").find("div", class_="name")
        assert ResultsTransformer._parse_seed_entry(name_div) == expected


class TestResultsTransformer:

    def test_completed_singles(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        match = _match_html()
        day = _day_html(1, "Mon, 02 February, 2026", match)
        page = _results_page(day)

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text(page, encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        parquet_path = tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        assert parquet_path.exists()

        df = pl.read_parquet(parquet_path)
        assert len(df) == 1

        row = df.row(0, named=True)
        assert row["winner_id"] == "AB12"
        assert row["loser_id"] == "CD34"
        assert row["match_status"] == "completed"
        assert row["round"] == "QF"
        assert row["court_name"] == "Court 1"
        assert row["duration_seconds"] == 5400
        assert row["w_set1"] == 6
        assert row["l_set1"] == 4
        assert row["w_set2"] == 7
        assert row["l_set2"] == 6
        assert row["tb_set2"] == 5
        assert row["score"] == "6-4 7-6(5)"
        assert row["match_code"] == "ms001"
        assert row["umpire"] == "John Doe"
        assert row["match_date"] == date(2026, 2, 2)
        assert row["tournament_day"] == 1

    def test_retirement(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        match = _match_html(
            winner_scores=((6,), (4,)),
            loser_scores=((3,), (5,)),
            notes="Game Set and Match A. Winner. A. Winner wins the match 6-3 4-5 RET.",
        )
        day = _day_html(3, "Wed, 04 February, 2026", match)
        page = _results_page(day)

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text(page, encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        )
        row = df.row(0, named=True)
        assert row["match_status"] == "retired"
        assert row["score"] == "6-3 4-5 RET"

    def test_walkover(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        match = _match_html(
            duration=None,
            winner_scores=(),
            loser_scores=(),
            notes="Winners: A. WINNER by Walkover",
            match_code=None,
        )
        day = _day_html(5, "Fri, 06 February, 2026", match)
        page = _results_page(day)

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text(page, encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        )
        row = df.row(0, named=True)
        assert row["match_status"] == "walkover"
        assert row["score"] == ""
        assert row["duration_seconds"] is None
        assert row["w_set1"] is None
        assert row["match_code"] is None

    def test_in_progress_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        match = _match_html(
            notes="A. Winner wins the point on his 1st serve.",
        )
        day = _day_html(7, "Sun, 08 February, 2026", match)
        page = _results_page(day)

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text(page, encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        parquet_path = tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        assert not parquet_path.exists()

    def test_doubles(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        match = _match_html(
            is_doubles=True,
            winner_partner=("ef56", "E. Partner"),
            loser_partner=("gh78", "G. Partner"),
            match_code="md001",
            notes="Game Set and Match Team A.",
        )
        day = _day_html(2, "Tue, 03 February, 2026", match)
        page = _results_page(day)

        doubles_path = tmp_path / "raw" / "atptour" / t.path / "results_doubles.html"
        doubles_path.parent.mkdir(parents=True, exist_ok=True)
        doubles_path.write_text(page, encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        )
        row = df.row(0, named=True)
        assert row["is_doubles"] is True
        assert row["winner_partner_id"] == "EF56"
        assert row["loser_partner_id"] == "GH78"
        assert row["match_code"] == "md001"

    def test_no_html_files_skips(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        transformer = ResultsTransformer(t)
        transformer.run()

        parquet_path = tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        assert not parquet_path.exists()

    def test_empty_page_skips(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text("<html><body></body></html>", encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        parquet_path = tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        assert not parquet_path.exists()

    def test_multiple_days(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        match1 = _match_html(
            round_text="Round of 32",
            winner_id="ab12",
            loser_id="cd34",
            match_code="ms001",
        )
        match2 = _match_html(
            round_text="Round of 16",
            winner_id="ef56",
            winner_name="E. Winner",
            loser_id="gh78",
            loser_name="G. Loser",
            match_code="ms005",
        )
        day1 = _day_html(1, "Mon, 02 February, 2026", match1)
        day2 = _day_html(2, "Tue, 03 February, 2026", match2)
        page = _results_page(day1 + day2)

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text(page, encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        )
        assert len(df) == 2
        assert set(df["tournament_day"].to_list()) == {1, 2}

    def test_tiebreak_on_loser_side(self, tmp_path, monkeypatch):
        """Tiebreak points appear on the set-tiebreak loser's score-item."""
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        t = _make_tournament()

        # Winner won 7-6(3): winner has (7,), loser has (6, 3)
        match = _match_html(
            winner_scores=((6,), (7,)),
            loser_scores=((3,), (6, 3)),
        )
        day = _day_html(1, "Mon, 02 February, 2026", match)
        page = _results_page(day)

        singles_path = tmp_path / "raw" / "atptour" / t.path / "results_singles.html"
        singles_path.parent.mkdir(parents=True, exist_ok=True)
        singles_path.write_text(page, encoding="utf-8")

        transformer = ResultsTransformer(t)
        transformer.run()

        df = pl.read_parquet(
            tmp_path / "stage" / "atptour" / t.path / "results.parquet"
        )
        row = df.row(0, named=True)
        assert row["w_set2"] == 7
        assert row["l_set2"] == 6
        assert row["tb_set2"] == 3
        assert row["score"] == "6-3 7-6(3)"
