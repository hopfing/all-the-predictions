import logging

from atp.base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class TournamentDiscovery(BaseExtractor):
    """
    Discover tournaments available from ATP Tour API or results archives.
    """

    def get_active_tournaments(self) -> list[tuple[int, int]]:
        """
        Fetch list of active tournaments from ATP live scores API returning minimal info
        required for downstream processing.

        :return: list of (tournament_id, year) tuples for active tournaments
        """

        scores_url = "https://app.atptour.com/api/v2/gateway/livematches/website"
        circuits = ["tour", "challenger"]

        results = []

        for circuit in circuits:
            url = f"{scores_url}?scoringTournamentLevel={circuit}"

            logger.info("Fetching %s tournaments", circuit)
            data = self.fetch_json(url)

            tournaments = data["Data"]["LiveMatchesTournamentsOrdered"]

            for t in tournaments:
                event_id = t["EventId"]
                event_year = t["EventYear"]

                if not isinstance(event_id, int) or not isinstance(event_year, int):
                    raise TypeError(
                        f"Expected int for EventId and EventYear, "
                        f"got {type(event_id).__name__} and {type(event_year).__name__} "
                        f"in tournament: {t}"
                    )

                results.append((event_id, event_year))

        logger.info("Found %d active tournaments", len(results))

        return results
