import logging
from datetime import date

import polars as pl
import requests

from atp.base_extractor import BaseExtractor
from atp.base_job import BaseJob
from atp.schemas import ROUND_DISPLAY_MAP, MatchStatsRecord
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


class MatchStatsStager(BaseJob):
    """Parse raw Hawkeye JSON files into per-match staged parquet files."""

    DOMAIN = "atptour"

    def __init__(self, tournament: Tournament):
        super().__init__()
        self.tournament = tournament

    def run(self):
        raw_dir = self._build_path("raw", self.tournament.path, "match_stats")
        json_files = self.list_files(raw_dir, "*.json")
        if not json_files:
            logger.info("No match stats JSONs for %s", self.tournament.logging_id)
            return

        stage_dir = self._build_path("stage", self.tournament.path, "match_stats")
        existing = {p.stem for p in self.list_files(stage_dir, "*.parquet")}
        to_process = [f for f in json_files if f.stem not in existing]

        logger.info(
            "%s: %d match stats JSONs, %d already staged, %d to process",
            self.tournament.logging_id,
            len(json_files),
            len(existing),
            len(to_process),
        )

        for json_path in to_process:
            data = self.read_json(json_path)
            records = self._parse_match(data, json_path.stem)
            if not records:
                continue

            df = pl.DataFrame([r.model_dump() for r in records])
            target = self._build_path(
                "stage", self.tournament.path, f"match_stats/{json_path.stem}.parquet"
            )
            self.save_parquet(df, target)

        logger.info("Staged match stats for %s", self.tournament.logging_id)

    def _parse_match(self, data: dict, match_code: str) -> list[MatchStatsRecord]:
        tournament_data = data["Tournament"]
        match = data["Match"]

        surface = tournament_data["Court"]
        tournament_start_date = date.fromisoformat(tournament_data["StartDate"][:10])
        tournament_end_date = date.fromisoformat(tournament_data["EndDate"][:10])

        round_name = match["RoundName"]
        if round_name not in ROUND_DISPLAY_MAP:
            raise ValueError(
                f"Unknown round '{round_name}' in {match_code} for "
                f"{self.tournament.logging_id}. Update ROUND_DISPLAY_MAP."
            )
        round_val = ROUND_DISPLAY_MAP[round_name]

        is_doubles = match["IsDoubles"]
        is_qualifier = match["IsQualifier"]
        court_name = match["CourtName"]
        best_of = match["NumberOfSets"]
        scoring_system = match["ScoringSystem"]
        reason = match["Reason"]
        tournament_day = int(match["DateSeq"])
        match_duration = self._parse_duration(match["MatchTime"])

        ump_first = match["UmpireFirstName"]
        ump_last = match["UmpireLastName"]
        umpire = f"{ump_first} {ump_last}" if ump_first and ump_last else None

        winning_id = match["WinningPlayerId"].upper()

        # Seed/entry from PlayerTeam1/PlayerTeam2 metadata containers
        pt1 = match["PlayerTeam1"]
        pt2 = match["PlayerTeam2"]
        seed_entry = {}
        for pt in (pt1, pt2):
            pid = pt["PlayerId"].upper()
            seed_entry[pid] = {
                "seed": int(pt["SeedPlayerTeam"]) if pt["SeedPlayerTeam"] else None,
                "entry": pt["EntryStatusPlayerTeam"],
            }

        player_team = match["PlayerTeam"]
        opponent_team = match["OpponentTeam"]

        teams = [
            (player_team, opponent_team),
            (opponent_team, player_team),
        ]

        records = []
        for my_team, their_team in teams:
            player = my_team["Player"]
            player_id = player["PlayerId"]
            player_name = f"{player['PlayerFirstName']} {player['PlayerLastName']}"

            opponent = their_team["Player"]
            opponent_id = opponent["PlayerId"]
            opponent_name = (
                f"{opponent['PlayerFirstName']} {opponent['PlayerLastName']}"
            )

            is_winner = player_id.upper() == winning_id

            player_partner_id = None
            player_partner_name = None
            opponent_partner_id = None
            opponent_partner_name = None
            if is_doubles:
                p_partner = my_team["Partner"]
                player_partner_id = p_partner["PlayerId"]
                player_partner_name = (
                    f"{p_partner['PlayerFirstName']} {p_partner['PlayerLastName']}"
                )

                o_partner = their_team["Partner"]
                opponent_partner_id = o_partner["PlayerId"]
                opponent_partner_name = (
                    f"{o_partner['PlayerFirstName']} {o_partner['PlayerLastName']}"
                )

            my_meta = seed_entry.get(player_id.upper(), {})
            opp_meta = seed_entry.get(opponent_id.upper(), {})

            for set_data in my_team["SetScores"]:
                stats = set_data["Stats"]
                if not stats:
                    continue

                set_num = set_data["SetNumber"]
                set_score = (
                    int(set_data["SetScore"])
                    if set_data["SetScore"] is not None
                    else None
                )
                tiebreak_score = set_data["TieBreakScore"]
                set_duration = self._parse_duration(stats["Time"])

                svc = stats["ServiceStats"]
                ret = stats["ReturnStats"]
                pts = stats["PointStats"]

                record = MatchStatsRecord(
                    tournament_id=self.tournament.tournament_id,
                    year=self.tournament.year,
                    surface=surface,
                    tournament_start_date=tournament_start_date,
                    tournament_end_date=tournament_end_date,
                    match_code=match_code,
                    round=round_val,
                    court_name=court_name,
                    is_doubles=is_doubles,
                    is_qualifier=is_qualifier,
                    match_duration_seconds=match_duration,
                    best_of=best_of,
                    scoring_system=scoring_system,
                    reason=reason,
                    tournament_day=tournament_day,
                    umpire=umpire,
                    set_num=set_num,
                    set_score=set_score,
                    tiebreak_score=tiebreak_score,
                    set_duration_seconds=set_duration,
                    player_id=player_id,
                    player_name=player_name,
                    opponent_id=opponent_id,
                    opponent_name=opponent_name,
                    is_winner=is_winner,
                    player_seed=my_meta.get("seed"),
                    player_entry=my_meta.get("entry"),
                    opponent_seed=opp_meta.get("seed"),
                    opponent_entry=opp_meta.get("entry"),
                    player_partner_id=player_partner_id,
                    player_partner_name=player_partner_name,
                    opponent_partner_id=opponent_partner_id,
                    opponent_partner_name=opponent_partner_name,
                    svc_games_played=svc["ServiceGamesPlayed"]["Number"],
                    svc_rating=svc["ServeRating"]["Number"],
                    svc_aces=svc["Aces"]["Number"],
                    svc_double_faults=svc["DoubleFaults"]["Number"],
                    svc_first_serve_in=svc["FirstServe"]["Dividend"],
                    svc_first_serve_att=svc["FirstServe"]["Divisor"],
                    svc_first_serve_in_pct=svc["FirstServe"]["Percent"],
                    svc_first_serve_pts_won=svc["FirstServePointsWon"]["Dividend"],
                    svc_first_serve_pts_played=svc["FirstServePointsWon"]["Divisor"],
                    svc_first_serve_pts_won_pct=svc["FirstServePointsWon"]["Percent"],
                    svc_second_serve_pts_won=svc["SecondServePointsWon"]["Dividend"],
                    svc_second_serve_pts_played=svc["SecondServePointsWon"]["Divisor"],
                    svc_second_serve_pts_won_pct=svc["SecondServePointsWon"]["Percent"],
                    svc_bp_saved=svc["BreakPointsSaved"]["Dividend"],
                    svc_bp_faced=svc["BreakPointsSaved"]["Divisor"],
                    svc_bp_saved_pct=svc["BreakPointsSaved"]["Percent"],
                    ret_games_played=ret["ReturnGamesPlayed"]["Number"],
                    ret_rating=ret["ReturnRating"]["Number"],
                    ret_first_serve_pts_won=ret["FirstServeReturnPointsWon"][
                        "Dividend"
                    ],
                    ret_first_serve_pts_played=ret["FirstServeReturnPointsWon"][
                        "Divisor"
                    ],
                    ret_first_serve_pts_won_pct=ret["FirstServeReturnPointsWon"][
                        "Percent"
                    ],
                    ret_second_serve_pts_won=ret["SecondServeReturnPointsWon"][
                        "Dividend"
                    ],
                    ret_second_serve_pts_played=ret["SecondServeReturnPointsWon"][
                        "Divisor"
                    ],
                    ret_second_serve_pts_won_pct=ret["SecondServeReturnPointsWon"][
                        "Percent"
                    ],
                    ret_bp_converted=ret["BreakPointsConverted"]["Dividend"],
                    ret_bp_opportunities=ret["BreakPointsConverted"]["Divisor"],
                    ret_bp_converted_pct=ret["BreakPointsConverted"]["Percent"],
                    pts_service_won=pts["TotalServicePointsWon"]["Dividend"],
                    pts_service_played=pts["TotalServicePointsWon"]["Divisor"],
                    pts_service_won_pct=pts["TotalServicePointsWon"]["Percent"],
                    pts_return_won=pts["TotalReturnPointsWon"]["Dividend"],
                    pts_return_played=pts["TotalReturnPointsWon"]["Divisor"],
                    pts_return_won_pct=pts["TotalReturnPointsWon"]["Percent"],
                    pts_total_won=pts["TotalPointsWon"]["Dividend"],
                    pts_total_played=pts["TotalPointsWon"]["Divisor"],
                    pts_total_won_pct=pts["TotalPointsWon"]["Percent"],
                )
                records.append(record)

        return records

    @staticmethod
    def _parse_duration(time_str: str | None) -> int | None:
        if not time_str:
            return None
        parts = time_str.split(":")
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])


class MatchStatsTransformer(BaseJob):
    """Concat per-match staged parquets into a single tournament-level parquet."""

    DOMAIN = "atptour"

    def __init__(self, tournament: Tournament):
        super().__init__()
        self.tournament = tournament

    def run(self):
        stage_dir = self._build_path("stage", self.tournament.path, "match_stats")
        parquets = self.list_files(stage_dir, "*.parquet")
        if not parquets:
            logger.warning("No staged match stats for %s", self.tournament.logging_id)
            return

        dfs = [pl.read_parquet(p) for p in parquets]
        combined = pl.concat(dfs, how="diagonal_relaxed")
        combined = self._add_derived_columns(combined)

        target = self._build_path("stage", self.tournament.path, "match_stats.parquet")
        self.save_parquet(combined, target)

        match_count = combined["match_code"].n_unique()
        logger.info(
            "Transformed %d rows (%d matches) for %s",
            len(combined),
            match_count,
            self.tournament.logging_id,
        )

    @staticmethod
    def _add_derived_columns(df: pl.DataFrame) -> pl.DataFrame:
        # sets_played: max set_num per match
        sets_played = df.group_by("match_code").agg(
            pl.col("set_num").max().alias("sets_played")
        )
        df = df.join(sets_played, on="match_code", how="left")

        # Self-join to get opponent's set_score and tiebreak_score
        opp = df.select(
            "match_code",
            "set_num",
            "player_id",
            pl.col("set_score").alias("opp_set_score"),
            pl.col("tiebreak_score").alias("opp_tiebreak_score"),
        )
        df = df.join(
            opp,
            left_on=["match_code", "set_num", "opponent_id"],
            right_on=["match_code", "set_num", "player_id"],
            how="left",
        )

        # won_set: did this player win the set? (null for set_num=0)
        df = df.with_columns(
            pl.when(pl.col("set_num") == 0)
            .then(None)
            .otherwise(pl.col("set_score") > pl.col("opp_set_score"))
            .alias("won_set")
        )

        # Tiebreak: the loser's score is in tiebreak_score or opp_tiebreak_score
        loser_tb = pl.coalesce("tiebreak_score", "opp_tiebreak_score")
        winner_tb = pl.when(loser_tb < 6).then(7).otherwise(loser_tb + 2)

        df = df.with_columns(
            # tiebreak_points_won: fill in for TB winner too
            pl.when(pl.col("tiebreak_score").is_not_null())
            .then(pl.col("tiebreak_score"))
            .when(pl.col("opp_tiebreak_score").is_not_null())
            .then(winner_tb)
            .otherwise(None)
            .cast(pl.Int64)
            .alias("tiebreak_points_won"),
            # tiebreak_points_played: total points in the tiebreak
            pl.when(loser_tb.is_not_null())
            .then(loser_tb + winner_tb)
            .otherwise(None)
            .cast(pl.Int64)
            .alias("tiebreak_points_played"),
        )

        return df.drop("opp_set_score", "opp_tiebreak_score")
