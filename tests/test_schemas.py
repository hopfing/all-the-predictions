from datetime import date, datetime

import pytest
from pydantic import ValidationError

from atp.schemas import (
    ROUND_DISPLAY_MAP,
    ResultsRecord,
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


def test_all_rounds_have_display_name():
    """Every Round member must appear as a value in ROUND_DISPLAY_MAP."""
    mapped_rounds = set(ROUND_DISPLAY_MAP.values())
    for member in Round:
        assert (
            member in mapped_rounds
        ), f"Round.{member.name} has no entry in ROUND_DISPLAY_MAP in atp/schemas.py."


def _results_kwargs(**overrides):
    """Base kwargs for a valid ResultsRecord (ATP singles, completed)."""
    defaults = dict(
        tournament_id=375,
        year=2026,
        match_date=date(2026, 2, 4),
        tournament_day=1,
        round=Round.QF,
        court_name="Court 1",
        is_doubles=False,
        match_status="completed",
        duration_seconds=5400,
        score="6-4 7-6(5)",
        w_set1=6,
        l_set1=4,
        w_set2=7,
        l_set2=6,
        tb_set2=5,
        winner_id="AB12",
        winner_name="A. Winner",
        loser_id="CD34",
        loser_name="C. Loser",
    )
    defaults.update(overrides)
    return defaults


class TestResultsRecord:
    def test_valid_singles(self):
        record = ResultsRecord(**_results_kwargs())
        assert record.winner_id == "AB12"
        assert record.match_uid == "2026_375_SGL_QF_AB12_CD34"

    def test_valid_doubles(self):
        record = ResultsRecord(
            **_results_kwargs(
                is_doubles=True,
                winner_partner_id="EF56",
                winner_partner_name="E. Partner",
                loser_partner_id="GH78",
                loser_partner_name="G. Partner",
            )
        )
        assert record.winner_partner_id == "EF56"
        assert record.match_uid == "2026_375_DBL_QF_AB12_CD34"

    def test_match_uid_sorted_ids(self):
        record = ResultsRecord(**_results_kwargs(winner_id="ZZ99", loser_id="AA01"))
        assert record.match_uid == "2026_375_SGL_QF_AA01_ZZ99"

    def test_uppercase_player_ids(self):
        record = ResultsRecord(**_results_kwargs(winner_id="ab12", loser_id="cd34"))
        assert record.winner_id == "AB12"
        assert record.loser_id == "CD34"

    def test_walkover(self):
        record = ResultsRecord(
            **_results_kwargs(
                match_status="walkover",
                duration_seconds=None,
                score="",
                w_set1=None,
                l_set1=None,
                w_set2=None,
                l_set2=None,
                tb_set2=None,
            )
        )
        assert record.match_status == "walkover"
        assert record.score == ""

    def test_walkover_with_duration_raises(self):
        with pytest.raises(ValidationError, match="Walkover must not have duration"):
            ResultsRecord(
                **_results_kwargs(
                    match_status="walkover",
                    score="",
                    w_set1=None,
                    l_set1=None,
                    w_set2=None,
                    l_set2=None,
                    tb_set2=None,
                )
            )

    def test_walkover_with_score_raises(self):
        with pytest.raises(ValidationError, match="Walkover must have empty score"):
            ResultsRecord(
                **_results_kwargs(
                    match_status="walkover",
                    duration_seconds=None,
                    w_set1=None,
                    l_set1=None,
                    w_set2=None,
                    l_set2=None,
                    tb_set2=None,
                )
            )

    def test_walkover_with_sets_raises(self):
        with pytest.raises(ValidationError, match="Walkover must not have set scores"):
            ResultsRecord(
                **_results_kwargs(
                    match_status="walkover",
                    duration_seconds=None,
                    score="",
                )
            )

    def test_set_gap_raises(self):
        with pytest.raises(ValidationError, match="Set gap"):
            ResultsRecord(
                **_results_kwargs(
                    w_set1=6,
                    l_set1=4,
                    w_set2=None,
                    l_set2=None,
                    tb_set2=None,
                    w_set3=6,
                    l_set3=3,
                )
            )

    def test_set_w_without_l_raises(self):
        with pytest.raises(ValidationError, match="w and l must both be present"):
            ResultsRecord(
                **_results_kwargs(
                    w_set1=6,
                    l_set1=None,
                    w_set2=None,
                    l_set2=None,
                    tb_set2=None,
                )
            )

    def test_retirement(self):
        record = ResultsRecord(
            **_results_kwargs(
                match_status="retired",
                score="6-3 4-5 RET",
                w_set1=6,
                l_set1=3,
                w_set2=4,
                l_set2=5,
                tb_set2=None,
            )
        )
        assert record.match_status == "retired"

    def test_doubles_requires_partners(self):
        with pytest.raises(ValidationError, match="winner_partner_id"):
            ResultsRecord(**_results_kwargs(is_doubles=True))

    def test_singles_rejects_partners(self):
        with pytest.raises(ValidationError, match="Singles match"):
            ResultsRecord(
                **_results_kwargs(
                    winner_partner_id="EF56", winner_partner_name="E. Partner"
                )
            )
