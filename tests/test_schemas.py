from atp.schemas import TournamentType, _TOURNAMENT_TYPE_CIRCUIT


def test_all_tournament_types_have_circuit_mapping():
    """Every TournamentType member must have a corresponding circuit mapping."""
    for member in TournamentType:
        assert (
            member in _TOURNAMENT_TYPE_CIRCUIT
        ), f"TournamentType.{member.name} is missing from _TOURNAMENT_TYPE_CIRCUIT in atp/schemas.py."
