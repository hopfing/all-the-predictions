import pytest

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


def _overview_data(**overrides) -> dict:
    """Minimal overview API response dict for from_overview_data tests."""
    base = {
        "EventType": "250",
        "Location": "Brisbane, Australia",
        "SponsorTitle": "Brisbane International",
    }
    base.update(overrides)
    return base


class TestFromOverviewData:

    def test_happy_path_tour(self):
        """Known tour-level EventType produces correct circuit and location."""
        data = _overview_data(EventType="250", Location="Brisbane, Australia")
        t = Tournament.from_overview_data(data, tournament_id=339, year=2026)

        assert t.tournament_id == 339
        assert t.year == 2026
        assert t.circuit == Circuit.TOUR
        assert t.location == "Brisbane, Australia"

    def test_happy_path_challenger(self):
        """Challenger EventType maps to CHALLENGER circuit."""
        data = _overview_data(EventType="CH", Location="Bergamo, Italy")
        t = Tournament.from_overview_data(data, tournament_id=1234, year=2026)

        assert t.circuit == Circuit.CHALLENGER

    def test_unknown_event_type_raises(self):
        """Unknown EventType raises ValueError with actionable message."""
        data = _overview_data(EventType="XYZ")

        with pytest.raises(ValueError, match="Unknown EventType 'XYZ'"):
            Tournament.from_overview_data(data, tournament_id=999, year=2026)

    def test_error_includes_tournament_context(self):
        """Error message includes tournament_id and sponsor title for debugging."""
        data = _overview_data(EventType="XYZ", SponsorTitle="Acme Open")

        with pytest.raises(ValueError, match=r"tournament 999.*Acme Open"):
            Tournament.from_overview_data(data, tournament_id=999, year=2026)

    def test_error_falls_back_to_location_when_no_sponsor(self):
        """When SponsorTitle is absent, error context falls back to Location."""
        data = {"EventType": "XYZ", "Location": "Nowhere, Mars"}

        with pytest.raises(ValueError, match="Nowhere, Mars"):
            Tournament.from_overview_data(data, tournament_id=999, year=2026)

    def test_location_no_comma_uses_full_string_as_name(self):
        """Location without a comma uses the full string as city/name."""
        data = _overview_data(Location="Montpellier")
        t = Tournament.from_overview_data(data, tournament_id=777, year=2026)

        assert t.name == "Montpellier"

    def test_location_multiple_commas_uses_first_segment(self):
        """Only the first segment before the comma is used as city."""
        data = _overview_data(Location="Washington, D.C., USA")
        t = Tournament.from_overview_data(data, tournament_id=888, year=2026)

        assert t.name == "Washington"
