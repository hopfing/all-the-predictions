import re
from datetime import date, datetime
from enum import Enum, auto

from pydantic import BaseModel, computed_field, field_validator, model_validator

from atp.player_id_corrections import correct_player_id


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


def _uppercase_or_none(v: str | None) -> str | None:
    if isinstance(v, str):
        return v.upper()
    return v


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


class StagedScheduleRecord(BaseModel):
    """Raw schedule data from a single HTML snapshot. One record per ATP match."""

    # Snapshot metadata
    snapshot_datetime: datetime

    # Tournament context
    tournament_id: int
    year: int

    # Time fields — raw strings from HTML data attributes
    match_date_str: str
    start_time_str: str
    time_suffix: str

    # Schedule context
    tournament_day: int
    court_name: str | None
    court_match_num: int
    round_text: str
    is_doubles: bool

    # Player 1
    p1_id: str | None = None
    p1_name: str | None = None
    p1_seed: int | None = None
    p1_entry: str | None = None
    p1_partner_id: str | None = None
    p1_partner_name: str | None = None

    # Player 2
    p2_id: str | None = None
    p2_name: str | None = None
    p2_seed: int | None = None
    p2_entry: str | None = None
    p2_partner_id: str | None = None
    p2_partner_name: str | None = None

    _uppercase_ids = field_validator(
        "p1_id", "p2_id", "p1_partner_id", "p2_partner_id", mode="before"
    )(_uppercase_or_none)

    @model_validator(mode="after")
    def _correct_player_ids(self):
        for field in ("p1_id", "p2_id", "p1_partner_id", "p2_partner_id"):
            val = getattr(self, field)
            if val is not None:
                setattr(
                    self, field, correct_player_id(val, self.tournament_id, self.year)
                )
        return self

    @model_validator(mode="after")
    def _validate_doubles_partners(self):
        if self.is_doubles:
            if self.p1_id is not None and self.p1_partner_id is None:
                raise ValueError("Doubles match with p1_id must have p1_partner_id")
            if self.p2_id is not None and self.p2_partner_id is None:
                raise ValueError("Doubles match with p2_id must have p2_partner_id")
        else:
            partner_fields = [
                self.p1_partner_id,
                self.p1_partner_name,
                self.p2_partner_id,
                self.p2_partner_name,
            ]
            if any(f is not None for f in partner_fields):
                raise ValueError("Singles match must not have partner fields")
        return self


class ScheduleRecord(BaseModel):
    """Consolidated schedule data. One record per unique match across all snapshots."""

    # Tournament context
    tournament_id: int
    year: int

    # Time fields — properly typed
    match_date: date
    start_time_utc: datetime | None = None
    time_estimated: bool

    # Schedule context
    tournament_day: int
    court_name: str | None
    court_match_num: int
    round: Round
    is_doubles: bool

    # Player 1 — required (TBD dropped)
    p1_id: str
    p1_name: str
    p1_seed: int | None = None
    p1_entry: str | None = None
    p1_partner_id: str | None = None
    p1_partner_name: str | None = None

    # Player 2 — required (TBD dropped)
    p2_id: str
    p2_name: str
    p2_seed: int | None = None
    p2_entry: str | None = None
    p2_partner_id: str | None = None
    p2_partner_name: str | None = None

    _uppercase_ids = field_validator(
        "p1_id", "p2_id", "p1_partner_id", "p2_partner_id", mode="before"
    )(_uppercase_or_none)

    @model_validator(mode="after")
    def _correct_player_ids(self):
        for field in ("p1_id", "p2_id", "p1_partner_id", "p2_partner_id"):
            val = getattr(self, field)
            if val is not None:
                setattr(
                    self, field, correct_player_id(val, self.tournament_id, self.year)
                )
        return self

    @model_validator(mode="after")
    def _validate_time_estimated(self):
        if not self.time_estimated and self.start_time_utc is None:
            raise ValueError("start_time_utc is required when time_estimated is False")
        return self

    @model_validator(mode="after")
    def _validate_doubles_partners(self):
        if self.is_doubles:
            if self.p1_partner_id is None:
                raise ValueError("Doubles match must have p1_partner_id")
            if self.p2_partner_id is None:
                raise ValueError("Doubles match must have p2_partner_id")
        else:
            partner_fields = [
                self.p1_partner_id,
                self.p1_partner_name,
                self.p2_partner_id,
                self.p2_partner_name,
            ]
            if any(f is not None for f in partner_fields):
                raise ValueError("Singles match must not have partner fields")
        return self

    @computed_field
    @property
    def match_uid(self) -> str:
        return create_match_uid(
            self.year, self.tournament_id, self.round, self.p1_id, self.p2_id
        )
