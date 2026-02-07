import logging
from pathlib import Path

import polars as pl
import requests

from atp.base_extractor import BaseExtractor
from atp.tournament.tournament import Tournament

logger = logging.getLogger(__name__)

HAWKEYE_BASE = "https://www.atptour.com/-/Hawkeye/MatchStats/Complete"


class MatchStatsExtractor(BaseExtractor):
    """Fetch per-match statistics JSON from the Hawkeye API."""

    DOMAIN = "atptour"

    def __init__(self):
        super().__init__()
        self.session.headers.update(
            {
                "Referer": "https://www.atptour.com/",
                "Origin": "https://www.atptour.com",
            }
        )

    def run(self, tournament: Tournament) -> None:
        match_codes = self._get_match_codes(tournament)
        if not match_codes:
            logger.info("No match codes for %s", tournament.logging_id)
            return

        stats_dir = self._build_path("raw", tournament.path, "match_stats")
        existing = {p.stem for p in self.list_files(stats_dir, "*.json")}
        to_fetch = [mc for mc in match_codes if mc not in existing]

        logger.info(
            "%s: %d match codes, %d already fetched, %d to fetch",
            tournament.logging_id,
            len(match_codes),
            len(existing),
            len(to_fetch),
        )

        failed = 0
        for match_code in to_fetch:
            url = f"{HAWKEYE_BASE}/{tournament.year}/{tournament.tournament_id}/{match_code}"
            target = self._build_path(
                "raw", tournament.path, f"match_stats/{match_code}.json"
            )

            try:
                data = self.fetch_json(url)
            except requests.HTTPError as e:
                if e.response is not None and e.response.status_code == 500:
                    logger.warning(
                        "Hawkeye 500 for %s match %s — skipping",
                        tournament.logging_id,
                        match_code,
                    )
                    failed += 1
                    continue
                raise

            self.save_json(data, target)

        logger.info(
            "%s: fetched %d match stats, %d failed",
            tournament.logging_id,
            len(to_fetch) - failed,
            failed,
        )

    def _get_match_codes(self, tournament: Tournament) -> list[str]:
        """Read match codes from results.parquet, excluding entries without a code."""
        results_path = self._build_path("stage", tournament.path, "results.parquet")
        if not results_path.exists():
            logger.warning(
                "No results.parquet for %s — cannot extract match codes",
                tournament.logging_id,
            )
            return []

        df = pl.read_parquet(results_path)
        codes = (
            df.filter(pl.col("match_code").is_not_null())
            .select("match_code")
            .unique()
            .sort("match_code")
            .to_series()
            .to_list()
        )
        return codes
