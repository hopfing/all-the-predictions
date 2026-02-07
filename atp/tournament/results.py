import logging
from pathlib import Path

from atp.base_extractor import BaseExtractor
from atp.schemas import Circuit
from atp.tournament.tournament import Tournament

logger = logging.getLogger(__name__)

_CIRCUIT_URL_PREFIX = {
    Circuit.TOUR: "current",
    Circuit.CHALLENGER: "current-challenger",
}


class ResultsExtractor(BaseExtractor):
    """Extract match results HTML from atptour.com (singles + doubles)."""

    DOMAIN = "atptour"

    def run(self, tournament: Tournament) -> None:
        logger.info("Fetching results for %s", tournament.logging_id)
        self._fetch_singles(tournament)
        self._fetch_doubles(tournament)
        logger.info("Saved results for %s", tournament.logging_id)

    def _fetch_singles(self, tournament: Tournament) -> Path:
        url = self._results_url(tournament)
        response = self._fetch(url)
        target = self._build_path("raw", tournament.path, "results_singles.html")
        return self.save_html(response.text, target)

    def _fetch_doubles(self, tournament: Tournament) -> Path:
        url = f"{self._results_url(tournament)}?matchType=doubles"
        response = self._fetch(url)
        target = self._build_path("raw", tournament.path, "results_doubles.html")
        return self.save_html(response.text, target)

    def _results_url(self, tournament: Tournament) -> str:
        prefix = _CIRCUIT_URL_PREFIX[tournament.circuit]
        return (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{tournament.url_slug}/{tournament.tournament_id}/results"
        )
