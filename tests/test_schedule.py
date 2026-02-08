from datetime import date, datetime, timedelta
from unittest.mock import MagicMock

import polars as pl
import pytest

from atp.schemas import Circuit, Round, StagedScheduleRecord
from atp.tournament.schedule import (
    ScheduleExtractor,
    ScheduleStager,
    ScheduleTransformer,
)
from atp.tournament.tournament import Tournament


class TestScoresUrlPrefix:

    def test_all_circuits_have_prefix(self):
        """Every Circuit member must produce a scores_url_prefix."""
        for circuit in Circuit:
            t = Tournament(
                tournament_id=1, year=2026, circuit=circuit, location="Test, XX"
            )
            assert isinstance(t.scores_url_prefix, str), (
                f"Circuit.{circuit.name} has no scores_url_prefix"
            )


class TestScheduleUrl:

    def test_tour_url(self):
        t = Tournament(
            tournament_id=339,
            year=2026,
            location="Brisbane, Australia",
            circuit=Circuit.TOUR,
        )
        prefix = t.scores_url_prefix
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
        prefix = t.scores_url_prefix
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


# --- ScheduleTransformer tests ---


def _staged_record(**overrides) -> dict:
    """Base kwargs for a valid staged record dict (ATP singles, Starts At)."""
    defaults = dict(
        snapshot_datetime=datetime(2026, 2, 6, 10, 0, 0),
        tournament_id=375,
        year=2026,
        match_date_str="2026-02-06",
        start_time_str="2026-02-06 11:30:00",
        time_suffix="Starts At",
        tournament_day=1,
        court_name="Court 1",
        court_match_num=1,
        round_text="R16",
        is_doubles=False,
        p1_id="AB12",
        p1_name="A. Player",
        p1_seed=None,
        p1_entry=None,
        p1_partner_id=None,
        p1_partner_name=None,
        p2_id="CD34",
        p2_name="C. Opponent",
        p2_seed=None,
        p2_entry=None,
        p2_partner_id=None,
        p2_partner_name=None,
    )
    defaults.update(overrides)
    return defaults


def _write_staged_parquet(
    tmp_path, tournament, records, filename="schedule_20260206_100000.parquet"
):
    """Write staged records as a parquet file in the expected location."""
    stage_dir = tmp_path / "stage" / "atptour" / tournament.path / "schedule"
    stage_dir.mkdir(parents=True, exist_ok=True)
    df = pl.DataFrame(records)
    path = stage_dir / filename
    df.write_parquet(path)
    return path


class TestScheduleTransformer:

    def test_basic_transform(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        _write_staged_parquet(tmp_path, _TOURNAMENT, [_staged_record()])

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        assert out.exists()
        df = pl.read_parquet(out)
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["p1_id"] == "AB12"
        assert row["round"] == "R16"
        assert row["match_date"] == date(2026, 2, 6)
        assert row["start_time_utc"] == datetime(2026, 2, 6, 11, 30, 0)
        assert row["time_estimated"] is False
        assert row["match_uid"] == "2026_375_SGL_R16_AB12_CD34"

    def test_tbd_matches_dropped(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        records = [
            _staged_record(),
            _staged_record(
                p1_id=None,
                p1_name=None,
                p2_id=None,
                p2_name=None,
                court_match_num=2,
                round_text="SF",
                start_time_str="",
                time_suffix="Followed By",
            ),
        ]
        _write_staged_parquet(tmp_path, _TOURNAMENT, records)

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        df = pl.read_parquet(out)
        assert len(df) == 1
        assert df["p1_id"][0] == "AB12"

    def test_dedup_keeps_latest_snapshot(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        old = _staged_record(
            snapshot_datetime=datetime(2026, 2, 6, 8, 0, 0),
            court_name="Court A",
        )
        new = _staged_record(
            snapshot_datetime=datetime(2026, 2, 6, 12, 0, 0),
            court_name="Court B",
        )
        _write_staged_parquet(
            tmp_path,
            _TOURNAMENT,
            [old],
            filename="schedule_20260206_080000.parquet",
        )
        _write_staged_parquet(
            tmp_path,
            _TOURNAMENT,
            [new],
            filename="schedule_20260206_120000.parquet",
        )

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        df = pl.read_parquet(out)
        assert len(df) == 1
        assert df["court_name"][0] == "Court B"

    def test_not_before_time_estimated(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        record = _staged_record(time_suffix="Not Before")
        _write_staged_parquet(tmp_path, _TOURNAMENT, [record])

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        df = pl.read_parquet(out)
        row = df.row(0, named=True)
        assert row["time_estimated"] is True
        assert row["start_time_utc"] == datetime(2026, 2, 6, 11, 30, 0)

    def test_followed_by_time_estimated_from_preceding(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        m1 = _staged_record(court_match_num=1, p1_id="AA11", p2_id="BB22")
        m2 = _staged_record(
            court_match_num=2,
            p1_id="CC33",
            p2_id="DD44",
            round_text="QF",
            start_time_str="",
            time_suffix="Followed By",
        )
        _write_staged_parquet(tmp_path, _TOURNAMENT, [m1, m2])

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        df = pl.read_parquet(out)
        followed_by = df.filter(pl.col("round") == "QF").row(0, named=True)
        assert followed_by["time_estimated"] is True
        expected = datetime(2026, 2, 6, 11, 30, 0) + timedelta(hours=2)
        assert followed_by["start_time_utc"] == expected

    def test_followed_by_no_preceding_stays_none(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        record = _staged_record(
            court_match_num=3,
            start_time_str="",
            time_suffix="Followed By",
        )
        _write_staged_parquet(tmp_path, _TOURNAMENT, [record])

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        df = pl.read_parquet(out)
        assert df["start_time_utc"][0] is None
        assert df["time_estimated"][0] is True

    def test_doubles_duration_estimate(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        m1 = _staged_record(
            court_match_num=1,
            is_doubles=True,
            p1_id="AA11",
            p1_partner_id="BB22",
            p1_name="A. One",
            p1_partner_name="B. Two",
            p2_id="CC33",
            p2_partner_id="DD44",
            p2_name="C. Three",
            p2_partner_name="D. Four",
        )
        m2 = _staged_record(
            court_match_num=2,
            p1_id="EE55",
            p2_id="FF66",
            p1_name="E. Five",
            p2_name="F. Six",
            round_text="QF",
            start_time_str="",
            time_suffix="Followed By",
        )
        _write_staged_parquet(tmp_path, _TOURNAMENT, [m1, m2])

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        df = pl.read_parquet(out)
        qf_row = df.filter(pl.col("round") == "QF").row(0, named=True)
        expected = datetime(2026, 2, 6, 11, 30, 0) + timedelta(hours=1, minutes=30)
        assert qf_row["start_time_utc"] == expected

    def test_no_staged_files_skips(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        assert not out.exists()

    def test_all_tbd_skips(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        record = _staged_record(p1_id=None, p1_name=None, p2_id=None, p2_name=None)
        _write_staged_parquet(tmp_path, _TOURNAMENT, [record])

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        assert not out.exists()

    def test_estimate_times_scoped_by_tournament_day(self, tmp_path, monkeypatch):
        """Time estimation should not chain across different tournament days."""
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)
        day1 = _staged_record(
            tournament_day=1,
            court_match_num=1,
            p1_id="AA11",
            p2_id="BB22",
        )
        day2 = _staged_record(
            tournament_day=2,
            court_match_num=1,
            p1_id="CC33",
            p2_id="DD44",
            round_text="QF",
            match_date_str="2026-02-07",
            start_time_str="",
            time_suffix="Followed By",
        )
        _write_staged_parquet(tmp_path, _TOURNAMENT, [day1, day2])

        transformer = ScheduleTransformer(_TOURNAMENT)
        transformer.run()

        out = tmp_path / "stage" / "atptour" / _TOURNAMENT.path / "schedule.parquet"
        df = pl.read_parquet(out)
        day2_row = df.filter(pl.col("round") == "QF").row(0, named=True)
        # Day 2 match 1 has no preceding match on day 2, so time stays None
        assert day2_row["start_time_utc"] is None
