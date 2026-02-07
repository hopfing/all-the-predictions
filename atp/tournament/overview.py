import logging
from pathlib import Path

import polars as pl

from atp.base_extractor import BaseExtractor
from atp.base_job import BaseJob
from atp.schemas import OverviewRecord
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


class OverviewTransformer(BaseJob):
    """Transform raw overview JSON into validated parquet."""

    DOMAIN = "atptour"

    def __init__(self, tournament: Tournament):
        super().__init__()
        self.tournament = tournament

    def run(self) -> Path:
        """
        Read raw overview JSON, validate with pydantic, and save as parquet.

        :return: path to saved parquet file
        """
        data = self.read_json("raw", self.tournament.path, "overview.json")
        data = {k: v.strip() if isinstance(v, str) else v for k, v in data.items()}

        # Extract city and country from location
        location_parts = data["Location"].split(",")
        city = location_parts[0].strip()
        country = location_parts[-1].strip() if len(location_parts) >= 2 else None
        if not country:
            country = None

        record = OverviewRecord(
            tournament_id=self.tournament.tournament_id,
            year=self.tournament.year,
            name=self.tournament.name,
            city=city,
            country=country,
            circuit=self.tournament.circuit,
            sponsor_title=data["SponsorTitle"],
            bio=data["Bio"],
            singles_draw_size=data["SinglesDrawSize"],
            doubles_draw_size=data["DoublesDrawSize"],
            surface=data["Surface"],
            surface_detail=data["SurfaceSubCat"],
            indoor=data["InOutdoor"],
            prize=data["Prize"],
            total_financial_commitment=data["TotalFinancialCommitment"],
            location=data["Location"],
            event_type=data["EventType"],
            event_type_detail=data["EventTypeDetail"],
            flag_url=data["FlagUrl"],
            website=data["Website"],
            website_url=data["WebsiteUrl"],
            fb_link=data["FbLink"],
            tw_link=data["TwLink"],
            ig_link=data["IgLink"],
            vixlet_url=data["VixletUrl"],
        )

        df = pl.DataFrame([record.model_dump(mode="json")])

        logger.info("Transformed overview for %s", self.tournament.logging_id)

        return self.save_parquet(df, "stage", self.tournament.path, "overview.parquet")
