"""Ultra-cautious HTTP client using curl_cffi with browser impersonation, session reuse, rate limiting, and backoff."""
import logging
import random
import time
from pathlib import Path
from typing import Optional

from curl_cffi import requests

from src.config import (
    DEFAULT_HEADERS,
    HLTV_BASE_URL,
    INITIAL_BACKOFF_SECONDS,
    MAX_REQUEST_DELAY_SECONDS,
    MAX_RETRIES,
    MIN_REQUEST_DELAY_SECONDS,
)
from src.utils.cache import DiskCache
from src.utils.session_manager import get_or_create_session

logger = logging.getLogger("cs2_pipeline.client")


class CloudflareChallengeError(Exception):
    """Raised when Cloudflare serves a CAPTCHA or Turnstile challenge."""
    pass


class RateLimitError(Exception):
    """Raised when rate limiting (429) occurs repeatedly."""
    pass


class StealthHLTVClient:
    """Cautious HTTP client that mimics a real desktop browser and persists session cookies."""

    def __init__(
        self,
        min_delay: float = MIN_REQUEST_DELAY_SECONDS,
        max_delay: float = MAX_REQUEST_DELAY_SECONDS,
        max_retries: int = MAX_RETRIES,
        initial_backoff: float = INITIAL_BACKOFF_SECONDS,
        auto_auth: bool = True,
    ):
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.initial_backoff = initial_backoff
        self.auto_auth = auto_auth

        self._session = requests.Session(impersonate="chrome150")
        self._last_request_time: float = 0.0
        self.consecutive_errors: int = 0
        self.user_agent = DEFAULT_HEADERS["User-Agent"]

        if self.auto_auth:
            self._init_session()

    def _init_session(self, force: bool = False) -> None:
        """Load or acquire active Cloudflare session cookies and matching User-Agent."""
        try:
            session_data = get_or_create_session(force=force)
            if session_data:
                if force:
                    try:
                        self._session.close()
                    except Exception:
                        pass
                    self._session = requests.Session(impersonate="chrome150")
                if "user_agent" in session_data:
                    self.user_agent = session_data["user_agent"]
                if "cookies" in session_data:
                    self._session.cookies.update(session_data["cookies"])
                    logger.info("Loaded %d session cookies into HTTP client.", len(session_data["cookies"]))
        except Exception as e:
            logger.warning("Could not auto-initialize session cookies: %s", e)

    def _apply_rate_limit(self) -> None:
        """Enforce human-like polite pacing between successive requests."""
        if self._last_request_time == 0.0:
            return

        elapsed = time.time() - self._last_request_time
        target_delay = random.uniform(self.min_delay, self.max_delay)

        if elapsed < target_delay:
            sleep_duration = target_delay - elapsed
            logger.info("Cautious delay: sleeping %.2fs before next request...", sleep_duration)
            time.sleep(sleep_duration)

    def _check_cloudflare(self, html: str, status_code: int) -> None:
        """Inspect HTML content for Cloudflare block / challenge signatures."""
        lower_html = html.lower()
        challenge_markers = [
            "just a moment...",
            "cf-turnstile",
            "challenges.cloudflare.com",
            "enable javascript and cookies to continue",
            "attention required! | cloudflare",
            "access denied",
        ]

        if status_code in (403, 503):
            for marker in challenge_markers:
                if marker in lower_html:
                    raise CloudflareChallengeError(
                        f"[ALERT] Cloudflare challenge detected (HTTP {status_code}) with signature '{marker}'."
                    )
            raise CloudflareChallengeError(
                f"[ALERT] HTTP {status_code} received from HLTV."
            )

        for marker in challenge_markers[:3]:
            if marker in lower_html and len(html) < 8000:
                raise CloudflareChallengeError(
                    f"[ALERT] Soft Cloudflare challenge detected with signature '{marker}'."
                )

    def get(
        self,
        url: str,
        referer: Optional[str] = None,
        cache_path: Optional[Path] = None,
    ) -> str:
        """Fetch a URL with caching, browser impersonation, and exponential backoff."""
        # 1. Check local disk cache first
        if cache_path and DiskCache.exists(cache_path):
            cached_content = DiskCache.read(cache_path)
            if cached_content:
                logger.info("Cache hit: loaded from %s (0 requests made)", cache_path.name)
                return cached_content

        headers = dict(DEFAULT_HEADERS)
        headers["User-Agent"] = self.user_agent
        if referer:
            headers["Referer"] = referer
        else:
            headers["Referer"] = HLTV_BASE_URL + "/"

        for attempt in range(1, self.max_retries + 1):
            self._apply_rate_limit()

            logger.info("Fetching [attempt %d/%d]: %s", attempt, self.max_retries, url)
            try:
                response = self._session.get(
                    url,
                    headers=headers,
                    timeout=30,
                )
                self._last_request_time = time.time()

                # Check for Cloudflare blockers
                self._check_cloudflare(response.text, response.status_code)

                if response.status_code == 429:
                    backoff = self.initial_backoff * (2 ** (attempt - 1)) + random.uniform(5, 15)
                    logger.warning(
                        "HTTP 429 Rate Limit. Initiating exponential backoff: sleeping %.1fs...",
                        backoff,
                    )
                    time.sleep(backoff)
                    continue

                if response.status_code == 200:
                    html_content = response.text
                    if cache_path:
                        DiskCache.write(cache_path, html_content)
                    self.consecutive_errors = 0
                    logger.info("Successfully fetched %d bytes (HTTP 200)", len(html_content))
                    return html_content

                logger.warning(
                    "Unexpected HTTP status %d on attempt %d: %s",
                    response.status_code,
                    attempt,
                    url,
                )

            except CloudflareChallengeError as e:
                logger.warning("Cloudflare challenge encountered: %s", e)
                if self.auto_auth and attempt <= 2:
                    logger.info("Attempting automated session refresh via Chrome CDP...")
                    self._init_session(force=True)
                    headers["User-Agent"] = self.user_agent
                    time.sleep(2.0)
                    continue
                else:
                    logger.critical("Circuit breaker tripped. Halting execution to protect IP!")
                    raise

            except Exception as e:
                logger.warning("Request error on attempt %d/%d: %s", attempt, self.max_retries, e)

            # Exponential backoff on generic failure
            backoff = self.initial_backoff * (attempt) + random.uniform(2, 6)
            logger.info("Backing off for %.1fs before retrying...", backoff)
            time.sleep(backoff)

        raise RuntimeError(f"Failed to fetch {url} after {self.max_retries} attempts.")

    def close(self) -> None:
        """Close the underlying session."""
        try:
            self._session.close()
        except Exception:
            pass
