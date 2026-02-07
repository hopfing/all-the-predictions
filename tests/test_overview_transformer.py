import json

import polars as pl
import pytest

from atp.schemas import Circuit, OverviewRecord, Surface
from atp.tournament.overview import OverviewTransformer
from atp.tournament.tournament import Tournament


SAMPLE_OVERVIEW = {
    "SponsorTitle": "Bergamo",
    "Bio": None,
    "SinglesDrawSize": 32,
    "DoublesDrawSize": 16,
    "Surface": "Hard",
    "Prize": "\u20ac145,250",
    "TotalFinancialCommitment": "\u20ac145,250",
    "Location": "Bergamo, Italy",
    "FlagUrl": "/-/media/images/flags/ita.svg",
    "Website": "www.internazionalidibergamo.it",
    "WebsiteUrl": "https://www.internazionalidibergamo.it",
    "InOutdoor": "I",
    "SurfaceSubCat": "Greenset",
    "EventType": "CH",
    "FbLink": "",
    "TwLink": "",
    "IgLink": "",
    "VixletUrl": "",
    "EventTypeDetail": 100,
}

TOURNAMENT = Tournament(
    tournament_id=9158,
    year=2026,
    city="Bergamo",
    circuit=Circuit.CHALLENGER,
)


class TestOverviewRecord:

    def test_valid_record(self):
        record = OverviewRecord(
            tournament_id=9158,
            year=2026,
            city="Bergamo",
            circuit=Circuit.CHALLENGER,
            sponsor_title="Bergamo",
            bio=None,
            singles_draw_size=32,
            doubles_draw_size=16,
            surface="Hard",
            surface_detail="Greenset",
            indoor="I",
            prize="\u20ac145,250",
            total_financial_commitment="\u20ac145,250",
            location="Bergamo, Italy",
            country="Italy",
            event_type="CH",
            event_type_detail=100,
            flag_url="/-/media/images/flags/ita.svg",
            website="www.internazionalidibergamo.it",
            website_url="https://www.internazionalidibergamo.it",
            fb_link="",
            tw_link="",
            ig_link="",
            vixlet_url="",
        )
        assert record.indoor is True
        assert record.surface == Surface.HARD

    def test_indoor_outdoor_parsing(self):
        """Field validator converts 'I' → True, 'O' → False."""
        record_data = dict(
            tournament_id=1,
            year=2026,
            city="Test",
            circuit=Circuit.TOUR,
            sponsor_title="T",
            bio=None,
            singles_draw_size=32,
            doubles_draw_size=16,
            surface="Clay",
            surface_detail="",
            prize="",
            total_financial_commitment="",
            location="Test, Country",
            country="Country",
            event_type="250",
            event_type_detail=0,
            flag_url="",
            website="",
            website_url="",
            fb_link="",
            tw_link="",
            ig_link="",
            vixlet_url="",
        )

        indoor = OverviewRecord(**{**record_data, "indoor": "I"})
        assert indoor.indoor is True

        outdoor = OverviewRecord(**{**record_data, "indoor": "O"})
        assert outdoor.indoor is False

    def test_invalid_indoor_value_raises(self):
        with pytest.raises(ValueError, match="Unknown InOutdoor"):
            OverviewRecord(
                tournament_id=1,
                year=2026,
                city="Test",
                circuit=Circuit.TOUR,
                sponsor_title="T",
                bio=None,
                singles_draw_size=32,
                doubles_draw_size=16,
                surface="Hard",
                surface_detail="",
                indoor="X",
                prize="",
                total_financial_commitment="",
                location="Test, Country",
                country="Country",
                event_type="250",
                event_type_detail=0,
                flag_url="",
                website="",
                website_url="",
                fb_link="",
                tw_link="",
                ig_link="",
                vixlet_url="",
            )

    def test_invalid_surface_raises(self):
        with pytest.raises(ValueError):
            OverviewRecord(
                tournament_id=1,
                year=2026,
                city="Test",
                circuit=Circuit.TOUR,
                sponsor_title="T",
                bio=None,
                singles_draw_size=32,
                doubles_draw_size=16,
                surface="Dirt",
                surface_detail="",
                indoor="I",
                prize="",
                total_financial_commitment="",
                location="Test, Country",
                country="Country",
                event_type="250",
                event_type_detail=0,
                flag_url="",
                website="",
                website_url="",
                fb_link="",
                tw_link="",
                ig_link="",
                vixlet_url="",
            )

    def test_model_dump_json_serializes_enums(self):
        """model_dump(mode='json') should serialize enums to string values."""
        record = OverviewRecord(
            tournament_id=1,
            year=2026,
            city="Test",
            circuit=Circuit.TOUR,
            sponsor_title="T",
            bio=None,
            singles_draw_size=32,
            doubles_draw_size=16,
            surface="Hard",
            surface_detail="",
            indoor="I",
            prize="",
            total_financial_commitment="",
            location="Test, Country",
            country="Country",
            event_type="250",
            event_type_detail=0,
            flag_url="",
            website="",
            website_url="",
            fb_link="",
            tw_link="",
            ig_link="",
            vixlet_url="",
        )
        dumped = record.model_dump(mode="json")
        assert dumped["circuit"] == "tour"
        assert dumped["surface"] == "Hard"


class TestOverviewTransformer:

    def test_run_produces_parquet(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        # Write raw JSON so transformer can read it
        raw_dir = tmp_path / "raw" / "atptour" / TOURNAMENT.path
        raw_dir.mkdir(parents=True)
        (raw_dir / "overview.json").write_text(json.dumps(SAMPLE_OVERVIEW))

        transformer = OverviewTransformer(TOURNAMENT)
        path = transformer.run()

        assert path.exists()
        assert path.suffix == ".parquet"

    def test_run_parquet_content(self, tmp_path, monkeypatch):
        monkeypatch.setattr("atp.base_job.DATA_ROOT", tmp_path)

        raw_dir = tmp_path / "raw" / "atptour" / TOURNAMENT.path
        raw_dir.mkdir(parents=True)
        (raw_dir / "overview.json").write_text(json.dumps(SAMPLE_OVERVIEW))

        transformer = OverviewTransformer(TOURNAMENT)
        path = transformer.run()

        df = pl.read_parquet(path)
        assert len(df) == 1
        row = df.row(0, named=True)
        assert row["tournament_id"] == 9158
        assert row["city"] == "Bergamo"
        assert row["circuit"] == "chal"
        assert row["surface"] == "Hard"
        assert row["indoor"] is True
        assert row["country"] == "Italy"
        assert row["sponsor_title"] == "Bergamo"
