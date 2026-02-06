import logging

from atp.base_extractor import BaseExtractor
from atp.tournament.tournament import Tournament

logger = logging.getLogger(__name__)


class OverviewExtractor(BaseExtractor):
    """Extract tournament metadata from ATP API."""

    DOMAIN = "atptour"

    def run(
        self,
        tournament_id: int,
        year: int,
    ) -> Tournament:
        """
        Extract overview JSON data, save raw JSON, and build Tournament object.

        Tournament overview endpoint is year-agnostic but downstream processing
        (e.g., schedules, results) requires a year, so we pass it in here.

        :param tournament_id: ATP tournament ID
        :param year: tournament year
        :return: Tournament object built from overview data
        """
        url = (
            f"https://www.atptour.com/en/-/tournaments/profile/{tournament_id}/overview"
        )

        logger.info(
            "Fetching overview for tournament %d (%d)",
            tournament_id,
            year,
        )

        data = self.fetch_json(url)

        tournament = Tournament.from_overview_data(
            data=data,
            tournament_id=tournament_id,
            year=year,
        )

        self.save_json(data, "raw", tournament.path, "overview.json")

        logger.info("Built Tournament object for %s", tournament.logging_id)

        return tournament
