from datetime import date, datetime

import pytest
from pydantic import ValidationError

from atp.schemas import (
    Round,
    ScheduleRecord,
    StagedScheduleRecord,
    TournamentType,
    _TOURNAMENT_TYPE_CIRCUIT,
    create_match_uid,
    Circuit,
    _CIRCUIT_DISPLAY,
)


def test_all_tournament_types_have_circuit_mapping():
    """Every TournamentType member must have a corresponding circuit mapping."""
    for member in TournamentType:
        assert (
            member in _TOURNAMENT_TYPE_CIRCUIT
        ), f"TournamentType.{member.name} is missing from _TOURNAMENT_TYPE_CIRCUIT in atp/schemas.py."


def test_all_circuits_have_display_name():
    """Every Circuit member must have a display name mapping."""
    for circuit in Circuit:
        assert circuit in _CIRCUIT_DISPLAY


class TestRound:
    def test_values_equal_names(self):
        for member in Round:
            assert member.value == member.name

    def test_is_str_enum(self):
        assert isinstance(Round.F, str)
        assert Round.QF == "QF"


class TestCreateMatchUid:
    def test_singles(self):
        uid = create_match_uid(2026, 375, Round.F, "AB12", "CD34", is_doubles=False)
        assert uid == "2026_375_SGL_F_AB12_CD34"

    def test_doubles(self):
        uid = create_match_uid(2026, 375, Round.F, "AB12", "CD34", is_doubles=True)
        assert uid == "2026_375_DBL_F_AB12_CD34"

    def test_player_ids_sorted(self):
        uid = create_match_uid(2026, 375, Round.F, "ZZ99", "AA01", is_doubles=False)
        assert uid == "2026_375_SGL_F_AA01_ZZ99"

    def test_uppercase_normalization(self):
        uid = create_match_uid(2026, 375, Round.SF, "ab12", "cd34", is_doubles=False)
        assert uid == "2026_375_SGL_SF_AB12_CD34"

    def test_round_value_in_uid(self):
        uid = create_match_uid(2026, 375, Round.R128, "AB12", "CD34", is_doubles=False)
        assert "R128" in uid

    def test_invalid_player_id_raises(self):
        with pytest.raises(ValueError, match="invalid match_uid"):
            create_match_uid(2026, 375, Round.F, "AB 12", "CD34", is_doubles=False)


def _staged_kwargs(**overrides):
    """Base kwargs for a valid StagedScheduleRecord (ATP singles)."""
    defaults = dict(
        snapshot_datetime=datetime(2026, 2, 4, 10, 0, 0),
        tournament_id=375,
        year=2026,
        match_date_str="2026-02-04",
        start_time_str="2026-02-04 11:30:00",
        time_suffix="Starts At",
        tournament_day=1,
        court_name="Court 1",
        court_match_num=1,
        round_text="R16",
        is_doubles=False,
        p1_id="AB12",
        p1_name="A. Player",
        p2_id="CD34",
        p2_name="C. Opponent",
    )
    defaults.update(overrides)
    return defaults


def _schedule_kwargs(**overrides):
    """Base kwargs for a valid ScheduleRecord (ATP singles, exact time)."""
    defaults = dict(
        tournament_id=375,
        year=2026,
        match_date=date(2026, 2, 4),
        start_time_utc=datetime(2026, 2, 4, 11, 30, 0),
        time_estimated=False,
        tournament_day=1,
        court_name="Court 1",
        court_match_num=1,
        round=Round.R16,
        is_doubles=False,
        p1_id="AB12",
        p1_name="A. Player",
        p2_id="CD34",
        p2_name="C. Opponent",
    )
    defaults.update(overrides)
    return defaults


class TestStagedScheduleRecord:
    def test_valid_singles(self):
        record = StagedScheduleRecord(**_staged_kwargs())
        assert record.p1_id == "AB12"
        assert record.round_text == "R16"

    def test_valid_doubles(self):
        record = StagedScheduleRecord(
            **_staged_kwargs(
                is_doubles=True,
                p1_partner_id="EF56",
                p1_partner_name="E. Partner",
                p2_partner_id="GH78",
                p2_partner_name="G. Partner",
            )
        )
        assert record.p1_partner_id == "EF56"

    def test_tbd_match(self):
        record = StagedScheduleRecord(
            **_staged_kwargs(p1_id=None, p1_name=None, p2_id=None, p2_name=None)
        )
        assert record.p1_id is None
        assert record.p2_id is None

    def test_uppercase_player_ids(self):
        record = StagedScheduleRecord(**_staged_kwargs(p1_id="ab12", p2_id="cd34"))
        assert record.p1_id == "AB12"
        assert record.p2_id == "CD34"

    def test_doubles_without_partner_raises(self):
        with pytest.raises(ValidationError, match="p1_partner_id"):
            StagedScheduleRecord(**_staged_kwargs(is_doubles=True))

    def test_singles_with_partner_raises(self):
        with pytest.raises(ValidationError, match="Singles match"):
            StagedScheduleRecord(
                **_staged_kwargs(p1_partner_id="EF56", p1_partner_name="E. Partner")
            )

    def test_followed_by_raw_time(self):
        record = StagedScheduleRecord(
            **_staged_kwargs(start_time_str="", time_suffix="Followed By")
        )
        assert record.start_time_str == ""
        assert record.time_suffix == "Followed By"


class TestScheduleRecord:
    def test_valid_singles(self):
        record = ScheduleRecord(**_schedule_kwargs())
        assert record.match_uid == "2026_375_SGL_R16_AB12_CD34"

    def test_match_uid_sorted_ids(self):
        record = ScheduleRecord(**_schedule_kwargs(p1_id="ZZ99", p2_id="AA01"))
        assert record.match_uid == "2026_375_SGL_R16_AA01_ZZ99"

    def test_valid_doubles(self):
        record = ScheduleRecord(
            **_schedule_kwargs(
                is_doubles=True,
                p1_partner_id="EF56",
                p1_partner_name="E. Partner",
                p2_partner_id="GH78",
                p2_partner_name="G. Partner",
            )
        )
        assert record.p1_partner_id == "EF56"
        assert record.match_uid == "2026_375_DBL_R16_AB12_CD34"

    def test_time_estimated_false_requires_time(self):
        with pytest.raises(ValidationError, match="start_time_utc is required"):
            ScheduleRecord(
                **_schedule_kwargs(time_estimated=False, start_time_utc=None)
            )

    def test_time_estimated_true_allows_none(self):
        record = ScheduleRecord(
            **_schedule_kwargs(time_estimated=True, start_time_utc=None)
        )
        assert record.start_time_utc is None

    def test_time_estimated_true_allows_time(self):
        record = ScheduleRecord(
            **_schedule_kwargs(
                time_estimated=True,
                start_time_utc=datetime(2026, 2, 4, 14, 0, 0),
            )
        )
        assert record.start_time_utc is not None

    def test_doubles_requires_partners(self):
        with pytest.raises(ValidationError, match="p1_partner_id"):
            ScheduleRecord(**_schedule_kwargs(is_doubles=True))

    def test_singles_rejects_partners(self):
        with pytest.raises(ValidationError, match="Singles match"):
            ScheduleRecord(
                **_schedule_kwargs(p1_partner_id="EF56", p1_partner_name="E. Partner")
            )

    def test_uppercase_player_ids(self):
        record = ScheduleRecord(**_schedule_kwargs(p1_id="ab12", p2_id="cd34"))
        assert record.p1_id == "AB12"
        assert record.p2_id == "CD34"
