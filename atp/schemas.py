from enum import Enum

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
    city: str
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
    country: str
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
