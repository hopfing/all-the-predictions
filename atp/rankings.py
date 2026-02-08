import logging
from datetime import date

import polars as pl
from bs4 import BeautifulSoup

from atp.base_extractor import BaseExtractor
from atp.base_job import BaseJob
from atp.schemas import RankingsRecord

logger = logging.getLogger(__name__)

RANKINGS_URL = "https://www.atptour.com/en/rankings/singles"


class RankingsExtractor(BaseExtractor):
    """Fetch weekly singles rankings HTML pages from atptour.com."""

    DOMAIN = "atptour"

    def __init__(self, start_year: int = 2025):
        super().__init__()
        self.start_year = start_year

    def run(self) -> None:
        # Fetch current page (top 100 only) to get the date dropdown
        discovery_url = f"{RANKINGS_URL}?rankRange=0-100"
        html = self.fetch_html(discovery_url)

        available = self._get_available_dates(html)
        target_dates = [d for d in available if d.year >= self.start_year]
        existing = self._get_existing_dates()
        to_fetch = [d for d in target_dates if d not in existing]

        logger.info(
            "Rankings: %d available dates, %d in range (>=%d), %d already fetched, %d to fetch",
            len(available),
            len(target_dates),
            self.start_year,
            len(existing),
            len(to_fetch),
        )

        for ranking_date in to_fetch:
            date_str = ranking_date.isoformat()
            url = f"{RANKINGS_URL}?rankRange=0-5000&dateWeek={date_str}"
            page_html = self.fetch_html(url)

            filename = f"rankings_singles_{ranking_date.strftime('%Y%m%d')}.html"
            target = self._build_path("raw", "rankings", filename)
            self.save_html(page_html, target)

        logger.info("Rankings: fetched %d new pages", len(to_fetch))

    def _get_available_dates(self, html: str) -> list[date]:
        """Parse the date dropdown to get all available ranking dates."""
        soup = BeautifulSoup(html, "lxml")
        dropdown = soup.select_one(
            'div.atp_filters-dropdown[data-key="DateWeek"] select'
        )
        if dropdown is None:
            raise ValueError(
                "Could not find DateWeek dropdown in rankings page. "
                "The page structure may have changed."
            )

        dates = []
        for option in dropdown.find_all("option"):
            value = option["value"]
            if value == "Current Week":
                continue
            dates.append(date.fromisoformat(value))

        return sorted(dates)

    def _get_existing_dates(self) -> set[date]:
        """Check which ranking dates already have saved HTML files."""
        rankings_dir = self._build_path("raw", "rankings")
        files = self.list_files(rankings_dir, "rankings_singles_*.html")
        dates = set()
        for f in files:
            # rankings_singles_YYYYMMDD.html
            date_str = f.stem.replace("rankings_singles_", "")
            dates.add(date(int(date_str[:4]), int(date_str[4:6]), int(date_str[6:8])))
        return dates


def _parse_date_from_stem(stem: str) -> date:
    """Parse YYYYMMDD from filename stem like 'rankings_singles_20260119'."""
    d = stem.replace("rankings_singles_", "")
    return date(int(d[:4]), int(d[4:6]), int(d[6:8]))


def _dash_to_none(text: str) -> int | None:
    """Parse an integer from text, treating '-' as None."""
    text = text.strip()
    if text == "-":
        return None
    return int(text.replace(",", ""))


class RankingsTransformer(BaseJob):
    """Parse rankings HTML pages into per-date parquet files."""

    DOMAIN = "atptour"

    def run(self) -> None:
        raw_dir = self._build_path("raw", "rankings")
        html_files = self.list_files(raw_dir, "rankings_singles_*.html")
        if not html_files:
            logger.info("No rankings HTML files to transform")
            return

        stage_dir = self._build_path("stage", "rankings")
        existing = {p.stem for p in self.list_files(stage_dir, "*.parquet")}
        to_process = [f for f in html_files if f.stem not in existing]

        logger.info(
            "Rankings transform: %d HTML files, %d already staged, %d to process",
            len(html_files),
            len(existing),
            len(to_process),
        )

        for html_path in to_process:
            ranking_date = _parse_date_from_stem(html_path.stem)
            html = self.read_html(html_path)
            records = self._parse_rankings_page(html, ranking_date)

            df = pl.DataFrame(
                [r.model_dump() for r in records], infer_schema_length=None
            )
            target = self._build_path("stage", "rankings", f"{html_path.stem}.parquet")
            self.save_parquet(df, target)

        logger.info("Rankings transform: staged %d new files", len(to_process))

    def _parse_rankings_page(
        self, html: str, ranking_date: date
    ) -> list[RankingsRecord]:
        soup = BeautifulSoup(html, "lxml")
        table = soup.select_one("table.mega-table.desktop-table.non-live")
        if table is None:
            raise ValueError(
                f"Could not find mega-table in rankings HTML for {ranking_date}. "
                "The page structure may have changed."
            )

        rows = table.select("tbody tr")
        records = []
        for tr in rows:
            rank_td = tr.select_one("td.rank")
            if rank_td is None:
                continue  # ad row injected into table
            rank_text = rank_td.get_text(strip=True)
            rank = int(rank_text.rstrip("T"))

            player_cell = tr.select_one("td.player")
            link = player_cell.select_one("li.name a")
            href = link["href"]
            # /en/players/{slug}/{id}/overview
            player_id = href.split("/")[-2]
            player_name = link.get_text(strip=True)

            flag_svg = player_cell.select_one("svg use")
            flag_href = flag_svg["href"]
            # .../flags.svg#flag-esp → esp
            nationality = flag_href.split("#flag-")[-1]

            age = int(tr.select_one("td.age").get_text(strip=True))

            points_text = tr.select_one("td.points").get_text(strip=True)
            points = int(points_text.replace(",", ""))

            # Rank movement: span.rank-up or span.rank-down, or empty
            rank_li = player_cell.select_one("li.rank")
            rank_up = rank_li.select_one("span.rank-up")
            rank_down = rank_li.select_one("span.rank-down")
            if rank_up:
                rank_move = int(rank_up.get_text(strip=True))
            elif rank_down:
                rank_move = -int(rank_down.get_text(strip=True))
            else:
                rank_move = None

            points_move = _dash_to_none(
                tr.select_one("td.pointsMove").get_text(strip=True)
            )
            tournaments_played = int(tr.select_one("td.tourns").get_text(strip=True))
            points_dropping = _dash_to_none(
                tr.select_one("td.drop").get_text(strip=True)
            )
            next_best = _dash_to_none(tr.select_one("td.best").get_text(strip=True))

            records.append(
                RankingsRecord(
                    ranking_date=ranking_date,
                    rank=rank,
                    player_id=player_id,
                    player_name=player_name,
                    nationality=nationality,
                    age=age,
                    points=points,
                    rank_move=rank_move,
                    points_move=points_move,
                    tournaments_played=tournaments_played,
                    points_dropping=points_dropping,
                    next_best=next_best,
                )
            )

        return records
