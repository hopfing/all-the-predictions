from datetime import datetime
from unittest.mock import MagicMock

import polars as pl
import pytest

from atp.schemas import Circuit
from atp.tournament.schedule import (
    ScheduleExtractor,
    ScheduleStager,
    _CIRCUIT_URL_PREFIX,
)
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


# --- ScheduleStager tests ---

_TOURNAMENT = Tournament(
    tournament_id=375,
    year=2026,
    location="Montpellier, France",
    circuit=Circuit.TOUR,
)


def _wrap_schedule_html(match_divs: str, day: int = 1) -> str:
    """Wrap match div(s) in a minimal valid schedule HTML page."""
    return f"""\
<html><body>
<div class="tournament-day">
  <h4 class="day"><span>Friday, February 6, 2026 (Day {day})</span></h4>
</div>
{match_divs}
</body></html>"""


def _singles_match_div(
    court: str | None = None,
    round_text: str = "R16",
    p1_id: str = "ab12",
    p1_name: str = "A. Player",
    p1_seed: str = "",
    p2_id: str = "cd34",
    p2_name: str = "C. Opponent",
    p2_seed: str = "",
    match_date: str = "2026-02-06",
    start_time: str = "2026-02-06 11:30:00",
    suffix: str = "Starts At",
    wta: bool = False,
) -> str:
    court_html = f"<span><strong>{court}</strong></span>" if court else ""
    wta_html = '<span class="match-type">WTA</span>' if wta else ""
    p1_rank = (
        f"<div class='rank'><span>{p1_seed}</span></div>"
        if p1_seed
        else "<div class='rank'><span></span></div>"
    )
    p2_rank = (
        f"<div class='rank'><span>{p2_seed}</span></div>"
        if p2_seed
        else "<div class='rank'><span></span></div>"
    )
    return f"""\
<div class="schedule" data-datetime="{start_time}" data-matchdate="{match_date}" data-suffix="{suffix}">
  <div class="schedule-header">
    <div class="schedule-location-timestamp">{court_html}</div>
    <div class="schedule-type">{round_text}</div>
  </div>
  <div class="schedule-content">
    <div class="schedule-type">{round_text}</div>
    <div class="schedule-players">
      <div class="player">
        <div class="name"><a href="/en/players/player/{p1_id}/overview">{p1_name}</a>{p1_rank}</div>
      </div>
      <div class="status">Vs</div>
      <div class="opponent">
        <div class="name"><a href="/en/players/opponent/{p2_id}/overview">{p2_name}</a>{p2_rank}</div>
      </div>
    </div>
    <div class="schedule-cta">{wta_html}</div>
  </div>
</div>"""


def _doubles_match_div(
    court: str | None = None,
    round_text: str = "R16",
    match_date: str = "2026-02-06",
    start_time: str = "2026-02-06 14:00:00",
    suffix: str = "Starts At",
) -> str:
    court_html = f"<span><strong>{court}</strong></span>" if court else ""
    return f"""\
<div class="schedule" data-datetime="{start_time}" data-matchdate="{match_date}" data-suffix="{suffix}">
  <div class="schedule-header">
    <div class="schedule-location-timestamp">{court_html}</div>
    <div class="schedule-type">{round_text}</div>
  </div>
  <div class="schedule-content">
    <div class="schedule-type">{round_text}</div>
    <div class="schedule-players">
      <div class="player">
        <div class="players">
          <div class="names">
            <div class="name"><a href="/en/players/p1/aa11/overview">A. One</a></div>
            <div class="name"><a href="/en/players/p2/bb22/overview">B. Two</a></div>
          </div>
          <div class="rank"><span>(1)</span></div>
        </div>
      </div>
      <div class="status">Vs</div>
      <div class="opponent">
        <div class="players">
          <div class="names">
            <div class="name"><a href="/en/players/p3/cc33/overview">C. Three</a></div>
            <div class="name"><a href="/en/players/p4/dd44/overview">D. Four</a></div>
          </div>
          <div class="rank"><span></span></div>
        </div>
      </div>
    </div>
  </div>
</div>"""


def _tbd_match_div(court: str | None = None) -> str:
    court_html = f"<span><strong>{court}</strong></span>" if court else ""
    return f"""\
<div class="schedule" data-datetime="" data-matchdate="2026-02-07" data-suffix="Followed By">
  <div class="schedule-header">
    <div class="schedule-location-timestamp">{court_html}</div>
    <div class="schedule-type">SF</div>
  </div>
  <div class="schedule-content">
    <div class="schedule-type">SF</div>
    <div class="schedule-players">
      <div class="possible-players-container">
        <div class="player possible">
          <div class="name"><a href="/en/players/p/xx99/overview">X. Maybe</a><div class="rank"><span></span></div></div>
        </div>
      </div>
      <div class="status">Vs</div>
      <div class="possible-players-container">
        <div class="opponent possible">
          <div class="name"><div class="rank"><span></span></div></div>
        </div>
      </div>
    </div>
  </div>
</div>"""


def _write_snapshot(tmp_path, tournament, html, timestamp="20260206_100000"):
    """Write HTML to the expected raw path and return the file path."""
    raw_dir = tmp_path / "raw" / "atptour" / tournament.path / "schedule"
    raw_dir.mkdir(parents=True, exist_ok=True)
    path = raw_dir / f"schedule_{timestamp}.html"
    path.write_text(html, encoding="utf-8")
    return path


class TestScheduleStager:

    def test_singles_match(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        html = _wrap_schedule_html(_singles_match_div(court="Court 1", p1_seed="(3)"))
        _write_snapshot(tmp_path, _TOURNAMENT, html)

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        files = list(parquet_dir.glob("*.parquet"))
        assert len(files) == 1

        df = pl.read_parquet(files[0])
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["p1_id"] == "AB12"
        assert row["p2_id"] == "CD34"
        assert row["round_text"] == "R16"
        assert row["court_name"] == "Court 1"
        assert row["court_match_num"] == 1
        assert row["p1_seed"] == 3
        assert row["tournament_day"] == 1
        assert row["time_suffix"] == "Starts At"

    def test_doubles_match(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        html = _wrap_schedule_html(_doubles_match_div(court="Court 2"))
        _write_snapshot(tmp_path, _TOURNAMENT, html)

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        df = pl.read_parquet(list(parquet_dir.glob("*.parquet"))[0])
        row = df.row(0, named=True)
        assert row["is_doubles"] is True
        assert row["p1_id"] == "AA11"
        assert row["p1_partner_id"] == "BB22"
        assert row["p2_id"] == "CC33"
        assert row["p2_partner_id"] == "DD44"
        assert row["p1_seed"] == 1

    def test_tbd_match_included(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        html = _wrap_schedule_html(_tbd_match_div(court="Center Court"))
        _write_snapshot(tmp_path, _TOURNAMENT, html)

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        df = pl.read_parquet(list(parquet_dir.glob("*.parquet"))[0])
        row = df.row(0, named=True)
        assert row["p1_id"] is None
        assert row["p2_id"] is None
        assert row["round_text"] == "SF"
        assert row["time_suffix"] == "Followed By"

    def test_wta_match_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        atp = _singles_match_div(court="Court 1")
        wta = _singles_match_div(wta=True)
        html = _wrap_schedule_html(atp + wta)
        _write_snapshot(tmp_path, _TOURNAMENT, html)

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        df = pl.read_parquet(list(parquet_dir.glob("*.parquet"))[0])
        assert len(df) == 1  # only ATP match

    def test_court_match_num_increments(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        m1 = _singles_match_div(court="Court 1", p1_id="aa11", p2_id="bb22")
        m2 = _singles_match_div(
            p1_id="cc33", p2_id="dd44", suffix="Followed By", start_time=""
        )
        html = _wrap_schedule_html(m1 + m2)
        _write_snapshot(tmp_path, _TOURNAMENT, html)

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        df = pl.read_parquet(list(parquet_dir.glob("*.parquet"))[0])
        assert df["court_match_num"].to_list() == [1, 2]
        assert df["court_name"].to_list() == ["Court 1", "Court 1"]

    def test_empty_schedule_skipped(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        html = '<html><body><div class="atp_scores-message_empty">No Daily Schedule</div></body></html>'
        _write_snapshot(tmp_path, _TOURNAMENT, html)

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        assert not parquet_dir.exists() or not list(parquet_dir.glob("*.parquet"))

    def test_snapshot_datetime_from_filename(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        html = _wrap_schedule_html(_singles_match_div(court="Court 1"))
        _write_snapshot(tmp_path, _TOURNAMENT, html, timestamp="20260206_143025")

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        df = pl.read_parquet(list(parquet_dir.glob("*.parquet"))[0])
        snap_dt = df["snapshot_datetime"][0]
        assert snap_dt == datetime(2026, 2, 6, 14, 30, 25)

    def test_multiple_courts(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        m1 = _singles_match_div(court="Court A", p1_id="aa11", p2_id="bb22")
        m2 = _singles_match_div(court="Court B", p1_id="cc33", p2_id="dd44")
        html = _wrap_schedule_html(m1 + m2)
        _write_snapshot(tmp_path, _TOURNAMENT, html)

        stager = ScheduleStager(_TOURNAMENT)
        stager.run()

        parquet_dir = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule"
        df = pl.read_parquet(list(parquet_dir.glob("*.parquet"))[0])
        assert df["court_name"].to_list() == ["Court A", "Court B"]
        assert df["court_match_num"].to_list() == [1, 1]


class TestParseSeedEntry:

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("1", (1, None)),
            ("(3)", (3, None)),
            ("WC", (None, "WC")),
            ("Q", (None, "Q")),
            ("1/Alt", (1, "Alt")),
            ("", (None, None)),
            (None, (None, None)),
            ("LL", (None, "LL")),
            ("PR", (None, "PR")),
        ],
    )
    def test_parse(self, value, expected):
        assert ScheduleStager._parse_seed_entry(value) == expected
