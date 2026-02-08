import logging
import re
from datetime import date, datetime, timedelta
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

from atp.base_extractor import BaseExtractor
from atp.base_job import BaseJob
from atp.schemas import (
    Round,
    ScheduleRecord,
    StagedScheduleRecord,
    create_match_uid,
    parse_seed_entry,
)
from atp.tournament.tournament import Tournament

logger = logging.getLogger(__name__)


class ScheduleExtractor(BaseExtractor):
    """Extract daily schedule HTML from atptour.com."""

    DOMAIN = "atptour"

    def run(self, tournament: Tournament) -> Path:
        """
        Fetch and save daily schedule HTML for a tournament.

        :param tournament: Tournament to fetch schedule for
        :return: path to saved HTML file
        """
        prefix = tournament.scores_url_prefix
        url = (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{tournament.url_slug}/{tournament.tournament_id}/daily-schedule"
        )

        logger.info("Fetching schedule for %s", tournament.logging_id)

        html = self.fetch_html(url)

        target = self._build_path(
            "raw",
            f"{tournament.path}/schedule",
            "schedule.html",
            version="datetime",
        )
        path = self.save_html(html, target)

        logger.info("Saved schedule for %s", tournament.logging_id)

        return path


class ScheduleStager(BaseJob):
    """Parse schedule HTML snapshots into staged parquet files."""

    DOMAIN = "atptour"

    def __init__(self, tournament: Tournament):
        super().__init__()
        self.tournament = tournament

    def run(self):
        """Parse all HTML snapshots and save as parquet files."""
        raw_dir = self._build_path("raw", f"{self.tournament.path}/schedule")
        html_files = self.list_files(raw_dir, "schedule_*.html")

        for html_path in html_files:
            records = self._parse_snapshot(html_path)
            if not records:
                logger.info(
                    "No ATP matches in %s for %s",
                    html_path.name,
                    self.tournament.logging_id,
                )
                continue

            df = pl.DataFrame([r.model_dump() for r in records])
            target = self._build_path(
                "stage",
                f"{self.tournament.path}/schedule",
                f"{html_path.stem}.parquet",
            )
            self.save_parquet(df, target)

        logger.info("Staged schedule snapshots for %s", self.tournament.logging_id)

    def _parse_snapshot(self, html_path: Path) -> list[StagedScheduleRecord]:
        """Parse a single HTML snapshot into StagedScheduleRecord list."""
        timestamp_str = html_path.stem.replace("schedule_", "")
        snapshot_dt = datetime.strptime(timestamp_str, "%Y%m%d_%H%M%S")

        html = self.read_html(html_path)
        soup = BeautifulSoup(html, "lxml")

        day_div = soup.find("div", class_="tournament-day")
        schedule_tags = soup.find_all("div", class_="schedule")
        match_tags = [t for t in schedule_tags if t.has_attr("data-datetime")]

        if day_div is None and not match_tags:
            return []

        day_span = day_div.find("h4", class_="day").find("span")
        day_match = re.search(r"\(Day (\d+)\)", day_span.text)
        tournament_day = int(day_match.group(1))

        records = []
        current_court = None
        court_match_num = 0

        for match_tag in match_tags:
            # Track court — strong tag only appears on first match per court
            loc_div = match_tag.find("div", class_="schedule-location-timestamp")
            strong = loc_div.find("strong") if loc_div else None
            if strong and strong.text.strip():
                current_court = strong.text.strip()
                court_match_num = 1
            else:
                court_match_num += 1

            # Skip matches without content (ITF stubs)
            content = match_tag.find("div", class_="schedule-content")
            if content is None:
                continue

            # Skip WTA matches
            match_type_span = content.find("span", class_="match-type")
            if match_type_span and match_type_span.text.strip() == "WTA":
                continue

            # Skip matches with empty round (cross-tournament refs, anomalies)
            round_div = content.find("div", class_="schedule-type")
            round_text = round_div.text.strip() if round_div else ""
            if not round_text:
                continue

            # Detect doubles
            player_divs = content.find("div", class_="schedule-players")
            is_doubles = bool(player_divs.find("div", class_="players"))

            # Parse player 1
            p1_tbd = bool(player_divs.find("div", class_="player possible"))
            if p1_tbd:
                p1_dict = {}
            else:
                p1_div = player_divs.find("div", class_="player")
                if is_doubles:
                    p1_dict = self._parse_doubles_team(p1_div, "p1")
                else:
                    p1_dict = self._parse_singles_player(p1_div, "p1")

            # Parse player 2
            p2_tbd = bool(player_divs.find("div", class_="opponent possible"))
            if p2_tbd:
                p2_dict = {}
            else:
                p2_div = player_divs.find("div", class_="opponent")
                if is_doubles:
                    p2_dict = self._parse_doubles_team(p2_div, "p2")
                else:
                    p2_dict = self._parse_singles_player(p2_div, "p2")

            record = StagedScheduleRecord(
                snapshot_datetime=snapshot_dt,
                tournament_id=self.tournament.tournament_id,
                year=self.tournament.year,
                match_date_str=match_tag["data-matchdate"],
                start_time_str=match_tag.get("data-datetime", ""),
                time_suffix=match_tag.get("data-suffix", ""),
                tournament_day=tournament_day,
                court_name=current_court,
                court_match_num=court_match_num,
                round_text=round_text,
                is_doubles=is_doubles,
                **p1_dict,
                **p2_dict,
            )
            records.append(record)

        return records

    def _parse_singles_player(self, player_div, prefix: str) -> dict:
        """Parse a singles player div into prefixed field dict."""
        name_div = player_div.find("div", class_="name")
        player_id, player_name = self._extract_player(name_div)

        rank_div = player_div.find("div", class_="rank")
        rank_text = rank_div.get_text(strip=True) if rank_div else None
        seed, entry = parse_seed_entry(rank_text)

        return {
            f"{prefix}_id": player_id,
            f"{prefix}_name": player_name,
            f"{prefix}_seed": seed,
            f"{prefix}_entry": entry,
        }

    def _parse_doubles_team(self, player_div, prefix: str) -> dict:
        """Parse a doubles team div into prefixed field dict."""
        players_container = player_div.find("div", class_="players")
        name_divs = players_container.find_all("div", class_="name")

        if len(name_divs) != 2:
            raise ValueError(
                f"Expected 2 players in doubles team, found {len(name_divs)} "
                f"in {self.tournament.logging_id}"
            )

        p1_id, p1_name = self._extract_player(name_divs[0])
        p2_id, p2_name = self._extract_player(name_divs[1])

        rank_div = player_div.find("div", class_="rank")
        rank_text = rank_div.get_text(strip=True) if rank_div else None
        seed, entry = parse_seed_entry(rank_text)

        return {
            f"{prefix}_id": p1_id,
            f"{prefix}_name": p1_name,
            f"{prefix}_partner_id": p2_id,
            f"{prefix}_partner_name": p2_name,
            f"{prefix}_seed": seed,
            f"{prefix}_entry": entry,
        }

    def _extract_player(self, name_div) -> tuple[str | None, str | None]:
        """Extract player ID and name from a name div."""
        name_link = name_div.find("a")
        if name_link and name_link.get("href"):
            player_id = name_link["href"].split("/")[-2]
            player_name = name_link.get_text(strip=True)
            return player_id, player_name
        return None, None


class ScheduleTransformer(BaseJob):
    """Consolidate staged schedule snapshots into a single tournament schedule."""

    DOMAIN = "atptour"

    _SINGLES_DURATION = timedelta(hours=2)
    _DOUBLES_DURATION = timedelta(hours=1, minutes=30)

    def __init__(self, tournament: Tournament):
        super().__init__()
        self.tournament = tournament

    def run(self):
        """Read staged parquets, consolidate, and save as single parquet."""
        stage_dir = self._build_path("stage", f"{self.tournament.path}/schedule")
        parquet_files = self.list_files(stage_dir, "schedule_*.parquet")

        if not parquet_files:
            logger.info("No staged schedule files for %s", self.tournament.logging_id)
            return

        dfs = [pl.read_parquet(f) for f in parquet_files]
        df = pl.concat(dfs, how="diagonal_relaxed")

        # Drop TBD matches (null player IDs)
        df = df.filter(pl.col("p1_id").is_not_null() & pl.col("p2_id").is_not_null())

        if df.is_empty():
            logger.info("No confirmed matches for %s", self.tournament.logging_id)
            return

        deduped = self._dedup_matches(df)
        transformed = [self._transform_row(row) for row in deduped]
        self._estimate_times(transformed)

        records = [ScheduleRecord(**row) for row in transformed]

        out_df = pl.DataFrame([r.model_dump() for r in records])
        target = self._build_path("stage", self.tournament.path, "schedule.parquet")
        self.save_parquet(out_df, target)

        logger.info(
            "Transformed %d matches for %s",
            len(records),
            self.tournament.logging_id,
        )

    def _parse_round(self, round_text: str) -> Round:
        try:
            return Round[round_text]
        except KeyError:
            raise ValueError(
                f"Unknown round_text '{round_text}' in schedule for "
                f"{self.tournament.logging_id}. Add member to Round enum in atp/schemas.py."
            )

    def _dedup_matches(self, df: pl.DataFrame) -> list[dict]:
        """Dedup by match_uid, keeping latest snapshot per match."""
        rows_by_uid = {}
        for row in df.iter_rows(named=True):
            round_enum = self._parse_round(row["round_text"])
            uid = create_match_uid(
                row["year"],
                row["tournament_id"],
                round_enum,
                row["p1_id"],
                row["p2_id"],
                row["is_doubles"],
            )
            existing = rows_by_uid.get(uid)
            if (
                existing is None
                or row["snapshot_datetime"] > existing["snapshot_datetime"]
            ):
                rows_by_uid[uid] = row
        return list(rows_by_uid.values())

    def _transform_row(self, row: dict) -> dict:
        """Transform a staged row dict into ScheduleRecord kwargs."""
        round_enum = self._parse_round(row["round_text"])
        match_date = date.fromisoformat(row["match_date_str"])

        time_suffix = row["time_suffix"]
        start_time_str = row["start_time_str"]

        if time_suffix == "Starts At" and start_time_str:
            time_estimated = False
            start_time_utc = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        elif time_suffix == "Not Before" and start_time_str:
            time_estimated = True
            start_time_utc = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
        else:
            time_estimated = True
            start_time_utc = None

        return {
            "tournament_id": row["tournament_id"],
            "year": row["year"],
            "match_date": match_date,
            "start_time_utc": start_time_utc,
            "time_estimated": time_estimated,
            "tournament_day": row["tournament_day"],
            "court_name": row["court_name"],
            "court_match_num": row["court_match_num"],
            "round": round_enum,
            "is_doubles": row["is_doubles"],
            "p1_id": row["p1_id"],
            "p1_name": row["p1_name"],
            "p1_seed": row["p1_seed"],
            "p1_entry": row["p1_entry"],
            "p1_partner_id": row["p1_partner_id"],
            "p1_partner_name": row["p1_partner_name"],
            "p2_id": row["p2_id"],
            "p2_name": row["p2_name"],
            "p2_seed": row["p2_seed"],
            "p2_entry": row["p2_entry"],
            "p2_partner_id": row["p2_partner_id"],
            "p2_partner_name": row["p2_partner_name"],
        }

    def _estimate_times(self, rows: list[dict]):
        """Estimate start times for matches without explicit times.

        Processes in court_match_num order so chained estimates work
        (e.g., match 3 can use match 2's estimated time).
        """
        by_court = {}
        for row in rows:
            key = (row["tournament_day"], row["court_name"], row["court_match_num"])
            by_court[key] = row

        for row in sorted(
            rows,
            key=lambda r: (
                r["tournament_day"],
                r["court_name"] or "",
                r["court_match_num"],
            ),
        ):
            if row["start_time_utc"] is not None:
                continue

            prev_key = (
                row["tournament_day"],
                row["court_name"],
                row["court_match_num"] - 1,
            )
            prev = by_court.get(prev_key)
            if prev and prev["start_time_utc"]:
                duration = (
                    self._DOUBLES_DURATION
                    if prev["is_doubles"]
                    else self._SINGLES_DURATION
                )
                row["start_time_utc"] = prev["start_time_utc"] + duration
