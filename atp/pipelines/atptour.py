import argparse
import logging

from atp.tournament.discovery import TournamentDiscovery

logger = logging.getLogger("atp.pipelines.atptour")


def parse_args():
    parser = argparse.ArgumentParser(description="Run data pipeline for ATPTour.com")

    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Set logging level",
    )

    return parser.parse_args()


def main():
    """CLI entry point for ATP Tour data pipeline."""

    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger.info("Discovering active tournaments")
    active = TournamentDiscovery().get_active_tournaments()
    if not active:
        logger.info("No active tournaments found")
        return


if __name__ == "__main__":
    main()
