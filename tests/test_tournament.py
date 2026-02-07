from atp.schemas import Circuit
from atp.tournament.tournament import Tournament


class TestTournamentUrlSlug:

    def test_simple_city(self):
        t = Tournament(
            tournament_id=339,
            year=2026,
            location="Brisbane, Australia",
            circuit=Circuit.TOUR,
        )
        assert t.url_slug == "brisbane"

    def test_multi_word_city(self):
        t = Tournament(
            tournament_id=100,
            year=2026,
            location="Buenos Aires, Argentina",
            circuit=Circuit.TOUR,
        )
        assert t.url_slug == "buenos-aires"

    def test_named_tournament(self):
        t = Tournament(
            tournament_id=580,
            year=2026,
            location="Melbourne, Australia",
            circuit=Circuit.TOUR,
        )
        assert t.url_slug == "australian-open"

    def test_apostrophe(self):
        t = Tournament(
            tournament_id=999,
            year=2026,
            location="Queen's Club, UK",
            circuit=Circuit.TOUR,
        )
        assert t.url_slug == "queens-club"

    def test_hyphen_preserved(self):
        t = Tournament(
            tournament_id=999,
            year=2026,
            location="Saint-Tropez, France",
            circuit=Circuit.TOUR,
        )
        assert t.url_slug == "saint-tropez"


class TestTournamentPath:

    def test_tour_city(self):
        """City-based tournament uses city as slug."""
        t = Tournament(
            tournament_id=339,
            year=2026,
            location="Brisbane, Australia",
            circuit=Circuit.TOUR,
        )
        assert t.path == "tournaments/tour/339_brisbane/2026"

    def test_grand_slam_named(self):
        """Grand Slam uses TOURNAMENT_NAMES mapping, not city."""
        t = Tournament(
            tournament_id=580,
            year=2026,
            location="Melbourne, Australia",
            circuit=Circuit.TOUR,
        )
        assert t.path == "tournaments/tour/580_australian_open/2026"

    def test_challenger(self):
        t = Tournament(
            tournament_id=1234,
            year=2026,
            location="Champaign, USA",
            circuit=Circuit.CHALLENGER,
        )
        assert t.path == "tournaments/chal/1234_champaign/2026"

    def test_name_with_spaces(self):
        """Multi-word names are slugified."""
        t = Tournament(
            tournament_id=9210,
            year=2026,
            location="Geneva, Switzerland",
            circuit=Circuit.TOUR,
        )
        # 9210 maps to "Laver Cup" in TOURNAMENT_NAMES
        assert t.path == "tournaments/tour/9210_laver_cup/2026"

    def test_name_with_hyphen(self):
        """Hyphens are replaced with underscores."""
        t = Tournament(
            tournament_id=999,
            year=2026,
            location="Saint-Tropez, France",
            circuit=Circuit.TOUR,
        )
        assert t.path == "tournaments/tour/999_saint_tropez/2026"
