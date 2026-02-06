from enum import Enum


class Circuit(Enum):
    TOUR = "tour"
    CHALLENGER = "chal"

    @property
    def display_name(self) -> str:
        return _CIRCUIT_DISPLAY[self]


_CIRCUIT_DISPLAY = {
    Circuit.TOUR: "ATP",
    Circuit.CHALLENGER: "Challenger",
}


class TournamentType(Enum):
    """
    Maps Overview API "EventType" values to standardized tournament types.
    Add new members here as new EventType values are encountered.
    """

    GS = "GS"
    ATP_250 = "250"
    ATP_500 = "500"
    CH = "CH"
    DCR = "DCR"

    @property
    def circuit(self) -> Circuit:
        return _TOURNAMENT_TYPE_CIRCUIT[self]


# Map TournamentType to circuit for storage and logging purposes.
_TOURNAMENT_TYPE_CIRCUIT = {
    TournamentType.GS: Circuit.TOUR,
    TournamentType.ATP_250: Circuit.TOUR,
    TournamentType.ATP_500: Circuit.TOUR,
    TournamentType.CH: Circuit.CHALLENGER,
    TournamentType.DCR: Circuit.TOUR,
}
