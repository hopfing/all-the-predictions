import logging
import random
import time

import requests

logger = logging.getLogger(__name__)


class BaseExtractor:
    """
    Base class for reusable data extraction functionality.
    """

    def __init__(
        self,
        timeout: int = 30,
    ):
        self.timeout = timeout
        self.session = self._create_session()

    def _create_session(self) -> requests.Session:
        """
        Create requests session with headers to mimic a real browser.
        """
        session = requests.Session()
        session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
            }
        )

        return session

    def _fetch(
        self,
        url: str,
        retries: int = 3,
    ) -> requests.Response:
        """
        Fetch a URL with pre-delay, retries, and exponential backoff to avoid throttling.
        """

        min_delay = 0.75
        max_delay = 1.25

        for attempt in range(retries + 1):
            try:
                time.sleep(random.uniform(min_delay, max_delay))
                logger.info("Fetching URL: %s", url)
                response = self.session.get(url, timeout=self.timeout)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning("Fetch failed: %s", e)
                min_delay *= 1.25
                max_delay *= 1.25
                if attempt == retries:
                    raise
