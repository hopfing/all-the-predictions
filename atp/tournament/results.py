import logging
import re
from datetime import date, datetime
from pathlib import Path

import polars as pl
from bs4 import BeautifulSoup

from atp.base_extractor import BaseExtractor
from atp.base_job import BaseJob
from atp.schemas import ROUND_DISPLAY_MAP, Circuit, ResultsRecord
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
        html = self.fetch_html(url)
        target = self._build_path("raw", tournament.path, "results_singles.html")
        return self.save_html(html, target)

    def _fetch_doubles(self, tournament: Tournament) -> Path:
        url = f"{self._results_url(tournament)}?matchType=doubles"
        html = self.fetch_html(url)
        target = self._build_path("raw", tournament.path, "results_doubles.html")
        return self.save_html(html, target)

    def _results_url(self, tournament: Tournament) -> str:
        prefix = _CIRCUIT_URL_PREFIX[tournament.circuit]
        return (
            f"https://www.atptour.com/en/scores/{prefix}/"
            f"{tournament.url_slug}/{tournament.tournament_id}/results"
        )


class ResultsTransformer(BaseJob):
    """Parse results HTML into validated ResultsRecord parquet."""

    DOMAIN = "atptour"

    def __init__(self, tournament: Tournament):
        super().__init__()
        self.tournament = tournament

    def run(self):
        singles_path = self._build_path(
            "raw", self.tournament.path, "results_singles.html"
        )
        doubles_path = self._build_path(
            "raw", self.tournament.path, "results_doubles.html"
        )

        records = []
        if singles_path.exists():
            records.extend(self._parse_html(singles_path, is_doubles=False))
        else:
            logger.warning("No singles results HTML for %s", self.tournament.logging_id)

        if doubles_path.exists():
            records.extend(self._parse_html(doubles_path, is_doubles=True))
        else:
            logger.warning("No doubles results HTML for %s", self.tournament.logging_id)

        if not records:
            logger.info("No results for %s", self.tournament.logging_id)
            return

        validated = [ResultsRecord(**r) for r in records]
        df = pl.DataFrame([r.model_dump() for r in validated])

        target = self._build_path("stage", self.tournament.path, "results.parquet")
        self.save_parquet(df, target)

        logger.info(
            "Transformed %d results for %s",
            len(validated),
            self.tournament.logging_id,
        )

    def _parse_html(self, html_path: Path, is_doubles: bool) -> list[dict]:
        html = self.read_html(html_path)
        soup = BeautifulSoup(html, "lxml")

        accordion_items = soup.find_all("div", class_="atp_accordion-item")
        if not accordion_items:
            logger.warning("No accordion items in %s", html_path.name)
            return []

        records = []
        for day_item in accordion_items:
            match_date, tournament_day = self._parse_day_header(day_item)
            match_divs = day_item.find_all("div", class_="match")

            for match_div in match_divs:
                record = self._parse_match(
                    match_div, match_date, tournament_day, is_doubles
                )
                if record is not None:
                    records.append(record)

        return records

    def _parse_day_header(self, day_item) -> tuple[date, int]:
        h4 = day_item.find("h4")
        day_span = h4.find("span")
        day_match = re.search(r"Day \((\d+)\)", day_span.text)
        tournament_day = int(day_match.group(1))

        date_text = h4.get_text()
        date_text = re.sub(r"Day \(\d+\)", "", date_text).strip()
        match_date = self._parse_date(date_text)
        return match_date, tournament_day

    @staticmethod
    def _parse_date(text: str) -> date:
        """Parse date from header like 'Sat, 07 February, 2026'."""
        # Remove day-of-week prefix
        parts = text.split(",", 1)
        if len(parts) == 2:
            text = parts[1].strip()
        # Now "07 February, 2026" — remove trailing comma artifacts
        text = text.replace(",", "")
        # "07 February 2026"
        return datetime.strptime(text, "%d %B %Y").date()

    def _parse_match(
        self, match_div, match_date: date, tournament_day: int, is_doubles: bool
    ) -> dict | None:
        # Determine match status from notes
        notes_div = match_div.find("div", class_="match-notes")
        notes_text = notes_div.get_text(strip=True) if notes_div else ""
        status = self._determine_status(notes_text)
        if status is None:
            return None  # in-progress, skip

        # Parse header: round, court, duration
        header = match_div.find("div", class_="match-header")
        round_enum, court_name = self._parse_round_court(header)
        duration_seconds = self._parse_duration(header)

        # Parse players/teams
        stats_items = match_div.find_all("div", class_="stats-item")
        winner_dict = None
        loser_dict = None

        for item in stats_items:
            player_info = item.find("div", class_="player-info")
            is_winner = player_info.find("div", class_="winner") is not None
            side = self._parse_side(item, is_doubles)

            if is_winner:
                winner_dict = side
            else:
                loser_dict = side

        if winner_dict is None or loser_dict is None:
            if status == "walkover" and winner_dict is not None:
                # Walkover — loser may still have player info but no winner flag
                # Find the non-winner stats-item
                for item in stats_items:
                    player_info = item.find("div", class_="player-info")
                    if player_info.find("div", class_="winner") is None:
                        loser_dict = self._parse_side(item, is_doubles)
                        break
            if loser_dict is None:
                logger.warning(
                    "Could not identify winner/loser in %s day %d, skipping",
                    self.tournament.logging_id,
                    tournament_day,
                )
                return None

        # Parse scores
        if status == "walkover":
            score_data = {
                "score": "",
                "w_set1": None,
                "l_set1": None,
                "tb_set1": None,
                "w_set2": None,
                "l_set2": None,
                "tb_set2": None,
                "w_set3": None,
                "l_set3": None,
                "tb_set3": None,
                "w_set4": None,
                "l_set4": None,
                "tb_set4": None,
                "w_set5": None,
                "l_set5": None,
                "tb_set5": None,
            }
        else:
            winner_scores_div = None
            loser_scores_div = None
            for item in stats_items:
                player_info = item.find("div", class_="player-info")
                is_winner = player_info.find("div", class_="winner") is not None
                scores_div = item.find("div", class_="scores")
                if is_winner:
                    winner_scores_div = scores_div
                else:
                    loser_scores_div = scores_div

            score_data = self._parse_scores(winner_scores_div, loser_scores_div, status)

        # Match code from Stats link
        match_code = self._parse_match_code(match_div)

        # Umpire
        umpire_div = match_div.find("div", class_="match-umpire")
        umpire = None
        if umpire_div:
            umpire_text = umpire_div.get_text(strip=True)
            if umpire_text.startswith("Ump:"):
                umpire = umpire_text[4:].strip()

        return {
            "tournament_id": self.tournament.tournament_id,
            "year": self.tournament.year,
            "match_date": match_date,
            "tournament_day": tournament_day,
            "round": round_enum,
            "court_name": court_name,
            "is_doubles": is_doubles,
            "match_status": status,
            "duration_seconds": duration_seconds if status != "walkover" else None,
            **score_data,
            "winner_id": winner_dict["id"],
            "winner_name": winner_dict["name"],
            "winner_seed": winner_dict.get("seed"),
            "winner_entry": winner_dict.get("entry"),
            "winner_partner_id": winner_dict.get("partner_id"),
            "winner_partner_name": winner_dict.get("partner_name"),
            "loser_id": loser_dict["id"],
            "loser_name": loser_dict["name"],
            "loser_seed": loser_dict.get("seed"),
            "loser_entry": loser_dict.get("entry"),
            "loser_partner_id": loser_dict.get("partner_id"),
            "loser_partner_name": loser_dict.get("partner_name"),
            "match_code": match_code,
            "umpire": umpire,
        }

    @staticmethod
    def _determine_status(notes_text: str) -> str | None:
        """Determine match completion status from match-notes text.

        Returns 'completed', 'retired', 'walkover', or None (in-progress/skip).
        """
        if "by Walkover" in notes_text:
            return "walkover"
        if notes_text.startswith("Game Set and Match"):
            if "RET" in notes_text:
                return "retired"
            return "completed"
        return None

    def _parse_round_court(self, header) -> tuple:
        """Extract Round enum and court name from match header."""
        strong = header.find("strong")
        text = strong.get_text(strip=True)

        if " - " in text:
            round_text, court_name = text.split(" - ", 1)
        else:
            round_text = text
            court_name = None

        round_enum = ROUND_DISPLAY_MAP[round_text]
        return round_enum, court_name

    @staticmethod
    def _parse_duration(header) -> int | None:
        """Extract duration in seconds from match header's second span."""
        spans = header.find_all("span", recursive=False)
        if len(spans) < 2:
            return None

        duration_text = spans[1].get_text(strip=True)
        if not duration_text:
            return None

        parts = duration_text.split(":")
        if len(parts) != 3:
            return None

        hours, minutes, seconds = int(parts[0]), int(parts[1]), int(parts[2])
        return hours * 3600 + minutes * 60 + seconds

    def _parse_side(self, stats_item, is_doubles: bool) -> dict:
        """Extract player(s) info from a stats-item div."""
        player_info = stats_item.find("div", class_="player-info")

        if is_doubles:
            return self._parse_doubles_side(player_info)
        return self._parse_singles_side(player_info)

    def _parse_singles_side(self, player_info) -> dict:
        name_div = player_info.find("div", class_="name")
        player_id, player_name = self._extract_player(name_div)
        seed, entry = self._parse_seed_entry(name_div)
        return {
            "id": player_id,
            "name": player_name,
            "seed": seed,
            "entry": entry,
        }

    def _parse_doubles_side(self, player_info) -> dict:
        players_div = player_info.find("div", class_="players")
        name_divs = players_div.find_all("div", class_="name")

        if len(name_divs) != 2:
            raise ValueError(
                f"Expected 2 players in doubles team, found {len(name_divs)} "
                f"in {self.tournament.logging_id}"
            )

        p1_id, p1_name = self._extract_player(name_divs[0])
        p2_id, p2_name = self._extract_player(name_divs[1])
        seed, entry = self._parse_seed_entry(name_divs[0])

        return {
            "id": p1_id,
            "name": p1_name,
            "partner_id": p2_id,
            "partner_name": p2_name,
            "seed": seed,
            "entry": entry,
        }

    @staticmethod
    def _extract_player(name_div) -> tuple[str, str]:
        """Extract player ID and name from a name div."""
        link = name_div.find("a")
        player_id = link["href"].split("/")[-2]
        player_name = link.get_text(strip=True)
        return player_id, player_name

    @staticmethod
    def _parse_seed_entry(name_div) -> tuple[int | None, str | None]:
        """Parse seed/entry from the span after the player name link."""
        span = name_div.find("span")
        if not span:
            return None, None

        value = span.get_text(strip=True).strip("()")
        if not value:
            return None, None

        if "/" in value:
            parts = value.split("/", 1)
            try:
                return int(parts[0]), parts[1] or None
            except ValueError:
                return None, value

        try:
            return int(value), None
        except ValueError:
            return None, value

    @staticmethod
    def _parse_scores(winner_scores_div, loser_scores_div, status: str) -> dict:
        """Extract structured set scores and build score string."""
        w_items = winner_scores_div.find_all("div", class_="score-item")[
            1:
        ]  # skip spacer
        l_items = loser_scores_div.find_all("div", class_="score-item")[1:]

        result = {}
        score_parts = []

        for i in range(5):
            set_num = i + 1
            w_key, l_key, tb_key = (
                f"w_set{set_num}",
                f"l_set{set_num}",
                f"tb_set{set_num}",
            )

            if i >= len(w_items) or i >= len(l_items):
                result[w_key] = None
                result[l_key] = None
                result[tb_key] = None
                continue

            w_spans = w_items[i].find_all("span")
            l_spans = l_items[i].find_all("span")

            if not w_spans or not l_spans:
                result[w_key] = None
                result[l_key] = None
                result[tb_key] = None
                continue

            w_games = int(w_spans[0].get_text(strip=True))
            l_games = int(l_spans[0].get_text(strip=True))
            result[w_key] = w_games
            result[l_key] = l_games

            # Tiebreak: second span on the losing side of the tiebreak
            tb = None
            if len(w_spans) > 1:
                tb = int(w_spans[1].get_text(strip=True))
            elif len(l_spans) > 1:
                tb = int(l_spans[1].get_text(strip=True))
            result[tb_key] = tb

            # Build score string part
            if tb is not None:
                score_parts.append(f"{w_games}-{l_games}({tb})")
            else:
                score_parts.append(f"{w_games}-{l_games}")

        score = " ".join(score_parts)
        if status == "retired":
            score += " RET"
        result["score"] = score

        return result

    @staticmethod
    def _parse_match_code(match_div) -> str | None:
        """Extract match code (e.g., ms001, md001) from Stats link."""
        cta = match_div.find("div", class_="match-cta")
        if not cta:
            return None

        for link in cta.find_all("a"):
            href = link.get("href", "")
            match = re.search(r"/(ms\d+|md\d+|qs\d+)$", href)
            if match:
                return match.group(1)
        return None
