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


class ScheduleExtractor(BaseExtractor):
    """Extract daily schedule HTML from atptour.com."""

    DOMAIN = "atptour"

    def run(self, tournament: Tournament) -> Path:
        """
        Fetch and save daily schedule HTML for a tournament.

        :param tournament: Tournament to fetch schedule for
        :return: path to saved HTML file
        """
        prefix = _CIRCUIT_URL_PREFIX[tournament.circuit]
        url = (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{tournament.url_slug}/{tournament.tournament_id}/daily-schedule"
        )

        logger.info("Fetching schedule for %s", tournament.logging_id)

        response = self._fetch(url)

        target = self._build_path(
            "raw",
            f"{tournament.path}/schedule",
            "schedule.html",
            version="datetime",
        )
        path = self.save_html(response.text, target)

        logger.info("Saved schedule for %s", tournament.logging_id)

        return path
