import pytest

from atp.schemas import (
    Round,
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
    def test_basic(self):
        uid = create_match_uid(2026, 375, Round.F, "AB12", "CD34")
        assert uid == "2026_375_F_AB12_CD34"

    def test_player_ids_sorted(self):
        uid = create_match_uid(2026, 375, Round.F, "ZZ99", "AA01")
        assert uid == "2026_375_F_AA01_ZZ99"

    def test_uppercase_normalization(self):
        uid = create_match_uid(2026, 375, Round.SF, "ab12", "cd34")
        assert uid == "2026_375_SF_AB12_CD34"

    def test_round_value_in_uid(self):
        uid = create_match_uid(2026, 375, Round.R128, "AB12", "CD34")
        assert "R128" in uid

    def test_invalid_player_id_raises(self):
        with pytest.raises(ValueError, match="invalid match_uid"):
            create_match_uid(2026, 375, Round.F, "AB 12", "CD34")
