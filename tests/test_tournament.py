from atp.schemas import Circuit
from atp.tournament.tournament import Tournament


class TestTournamentPath:

    def test_tour_city(self):
        """City-based tournament uses city as slug."""
        t = Tournament(tournament_id=339, year=2026, city="Brisbane", circuit=Circuit.TOUR)
        assert t.path == "tournaments/tour/339_brisbane/2026"

    def test_grand_slam_named(self):
        """Grand Slam uses TOURNAMENT_NAMES mapping, not city."""
        t = Tournament(tournament_id=580, year=2026, city="Melbourne", circuit=Circuit.TOUR)
        assert t.path == "tournaments/tour/580_australian_open/2026"

    def test_challenger(self):
        t = Tournament(tournament_id=1234, year=2026, city="Champaign", circuit=Circuit.CHALLENGER)
        assert t.path == "tournaments/chal/1234_champaign/2026"

    def test_name_with_spaces(self):
        """Multi-word names are slugified."""
        t = Tournament(tournament_id=9210, year=2026, city="Geneva", circuit=Circuit.TOUR)
        # 9210 maps to "Laver Cup" in TOURNAMENT_NAMES
        assert t.path == "tournaments/tour/9210_laver_cup/2026"

    def test_name_with_hyphen(self):
        """Hyphens are replaced with underscores."""
        t = Tournament(tournament_id=999, year=2026, city="Saint-Tropez", circuit=Circuit.TOUR)
        assert t.path == "tournaments/tour/999_saint_tropez/2026"
