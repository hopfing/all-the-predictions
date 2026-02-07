import re
from enum import Enum, auto

from pydantic import BaseModel, field_validator


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


class Surface(Enum):
    """
    Court surface types. Add new members here as new values are encountered.
    """

    HARD = "Hard"
    CLAY = "Clay"
    GRASS = "Grass"
    CARPET = "Carpet"


class Round(str, Enum):
    def _generate_next_value_(name, start, count, last_values):
        return name

    F = auto()
    SF = auto()
    QF = auto()
    R16 = auto()
    R32 = auto()
    R64 = auto()
    R128 = auto()
    RR = auto()
    Q3 = auto()
    Q2 = auto()
    Q1 = auto()
    BRONZE = auto()
    THIRDPLACE = auto()


MATCH_UID_PATTERN = re.compile(r"^\d{4}_\d+_[A-Z0-9]+_[A-Z0-9]+_[A-Z0-9]+$")


def create_match_uid(
    year: int,
    tournament_id: int,
    round: Round,
    p1_id: str,
    p2_id: str,
) -> str:
    """Create stable match UID for joining across datasets.

    Format: {year}_{tournament_id}_{round}_{sorted_player_ids}
    Player IDs are sorted alphabetically for consistency regardless of
    which side a player appears on.
    """
    player_ids = "_".join(sorted([p1_id.upper(), p2_id.upper()]))
    match_uid = f"{year}_{tournament_id}_{round.value}_{player_ids}"
    if not MATCH_UID_PATTERN.match(match_uid):
        raise ValueError(f"Generated invalid match_uid: {match_uid}")
    return match_uid


_INDOOR_MAP = {"I": True, "O": False}


def _empty_to_none(v):
    """Convert empty strings to None for fields that expect specific values or None.

    Use on optional enum/constrained fields (e.g., Surface) where an empty string
    from the API would otherwise bypass validation instead of becoming None.
    """
    if v == "":
        return None
    return v


class OverviewRecord(BaseModel):
    """
    Validated schema for transformed overview data.

    Combines tournament identity with overview API fields for self-describing parquet output.
    """

    # Tournament identity
    tournament_id: int
    year: int
    name: str
    city: str
    country: str | None
    circuit: Circuit

    # Overview fields
    sponsor_title: str
    bio: str | None
    singles_draw_size: int
    doubles_draw_size: int
    surface: Surface | None  # None for multi-location events (e.g., Davis Cup)
    surface_detail: str | None
    indoor: bool  # "I" → True, "O" → False
    prize: str
    total_financial_commitment: str
    location: str
    event_type: str
    event_type_detail: int
    flag_url: str | None
    website: str
    website_url: str
    fb_link: str
    tw_link: str
    ig_link: str
    vixlet_url: str

    _normalize_empty = field_validator("surface", mode="before")(_empty_to_none)

    @field_validator("indoor", mode="before")
    @classmethod
    def parse_indoor(cls, v):
        if v in _INDOOR_MAP:
            return _INDOOR_MAP[v]
        if isinstance(v, bool):
            return v
        raise ValueError(
            f"Unknown InOutdoor value '{v}'. Expected 'I' or 'O'. "
            f"Update _INDOOR_MAP in atp/schemas.py."
        )
