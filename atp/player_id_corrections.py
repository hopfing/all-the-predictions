"""
Manual corrections for malformed player IDs in ATPTour.com source data.

Some players (typically young/newer) appear with Sportradar-format IDs
(e.g., SR:COMPETITOR:972327) instead of standard 4-character ATP codes.
Add corrections here as they are encountered.
"""

import logging

logger = logging.getLogger(__name__)

# (bad_id_uppercased, tournament_id, year) -> correct_4char_id
_CORRECTIONS = {
    # https://www.atptour.com/en/scores/match-stats/archive/2026/580/qs030
    ("SR:COMPETITOR:972327", 580, 2026): "J0DZ",
    # https://www.atptour.com/en/scores/match-stats/archive/2026/580/qs077
    ("SR:COMPETITOR:1055851", 580, 2026): "H0K0",
    # https://www.atptour.com/en/players/pruchya-isaro/i326/overview
    ("SR:COMPETITOR:59700", 580, 2026): "I326",
    # https://www.atptour.com/en/players/niki-kaliyanda-poonacha/kh77/overview
    ("SR:COMPETITOR:145936", 580, 2026): "KH77",
    # Davis Cup Qualifiers 1st Rd 2026
    ("SR:COMPETITOR:637610", 8096, 2026): "O0BI",  # Carl Emil Overbeck
    ("SR:COMPETITOR:1021133", 8096, 2026): "M0UR",  # Ognjen Milic
    ("SR:COMPETITOR:915589", 8096, 2026): "V0GR",  # Alexander Vasilev
    ("SR:COMPETITOR:915951", 8096, 2026): "R0IL",  # Iliyan Radulov
    ("SR:COMPETITOR:617530", 8096, 2026): "W0BU",  # Olle Wallin
    ("SR:COMPETITOR:168420", 8096, 2026): "AG08",  # Andres Andrade
}


def correct_player_id(player_id: str, tournament_id: int, year: int) -> str:
    """Apply corrections for malformed player IDs.

    Returns the corrected ID if a mapping exists, otherwise returns
    the original ID unchanged and logs a warning for colon-containing IDs.
    """
    if ":" not in player_id:
        return player_id

    key = (player_id.upper(), tournament_id, year)
    if key in _CORRECTIONS:
        corrected = _CORRECTIONS[key]
        logger.info("Corrected player ID: %s -> %s", player_id, corrected)
        return corrected

    logger.warning(
        "Uncorrected player ID: %s (tournament=%d, year=%d). "
        "Add correction to atp/player_id_corrections.py.",
        player_id,
        tournament_id,
        year,
    )
    return player_id
