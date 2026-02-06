from dataclasses import dataclass

from atp.schemas import Circuit, TournamentType


@dataclass(frozen=True)
class Tournament:
    """
    Tournament metadata container.

    Encapsulates all tournament identification to eliminate need for parameter threading
    across pipeline modules.
    """

    tournament_id: int
    year: int
    city: str
    circuit: Circuit

    @property
    def logging_id(self) -> str:
        """Human-readable identifier for logging: 'ATP Brisbane 2026 (339)'"""
        return f"{self.circuit.display_name} {self.city} {self.year} ({self.tournament_id})"

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
        city = data["Location"].split(",")[0].strip()

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
            city=city,
            circuit=circuit,
            year=year,
        )
