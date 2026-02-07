from dataclasses import dataclass

from atp.schemas import Circuit, TournamentType


# Display names for tournaments known by name rather than city
TOURNAMENT_NAMES: dict[int, str] = {
    9900: "United Cup",
    580: "Australian Open",
    8096: "Davis Cup Qualifiers 1st Rd",
    520: "Roland Garros",
    540: "Wimbledon",
    560: "US Open",
    8097: "Davis Cup Qualifiers 2nd Rd",
    9210: "Laver Cup",
    605: "Nitto ATP Finals",
    8099: "Davis Cup Finals",
}


@dataclass(frozen=True)
class Tournament:
    """
    Tournament metadata container.

    Encapsulates all tournament identification to eliminate need for parameter threading
    across pipeline modules.
    """

    tournament_id: int
    year: int
    circuit: Circuit
    location: str

    @property
    def name(self) -> str:
        """Display name - uses TOURNAMENT_NAMES mapping or falls back to city."""
        city = self.location.split(",")[0].strip()

        name = TOURNAMENT_NAMES.get(self.tournament_id, city)

        if name == "Multiple Locations":
            raise ValueError(
                f"Unable to determine tournament name for ID {self.tournament_id} with location '{self.location}'.\n"
                f"Add entry to TOURNAMENT_NAMES in tournament.py."
            )

        return name

    @property
    def url_slug(self) -> str:
        """URL-friendly slug for atptour.com paths."""

        return self.name.lower().replace("'", "").replace(" ", "-")

    @property
    def path(self) -> str:
        """
        Storage path segment for tournament-scoped files.

        Format: tournaments/{circuit}/{tid}_{name_slug}/{year}
        Example: tournaments/tour/580_australian_open/2026
        """
        path_slug = self.url_slug.replace("-", "_")

        return f"tournaments/{self.circuit.value}/{self.tournament_id}_{path_slug}/{self.year}"

    @property
    def logging_id(self) -> str:
        """
        Human-readable identifier for logging:
            'ATP Brisbane 2026 (339)'
            'ATP Australian Open 2026 (580)'
        """
        return f"{self.circuit.display_name} {self.name} {self.year} ({self.tournament_id})"

    @classmethod
    def from_overview_data(
        cls,
        data: dict,
        tournament_id: int,
        year: int,
    ) -> "Tournament":
        """
        Build Tournament instance from overview API response.

        :param data: JSON response from ATP API Overview endpoint
        :param tournament_id: ATP tournament ID
        :param year: tournament year
        :return: Tournament instance
        """
        try:
            tournament_type = TournamentType(data["EventType"])
        except ValueError:
            raise ValueError(
                f"Unknown EventType '{data['EventType']}' for tournament {tournament_id} "
                f"({data.get('SponsorTitle') or data.get('Location', 'unknown')}). "
                f"Add member to TournamentType in atp/schemas.py."
            )
        circuit = tournament_type.circuit

        return cls(
            tournament_id=tournament_id,
            location=data["Location"],
            circuit=circuit,
            year=year,
        )
