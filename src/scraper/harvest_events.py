"""Harvest all CS2 MVP events from HLTV with pagination, stealth client, and disk caching."""
import json
import logging
import re
import sys
from bs4 import BeautifulSoup

from src.config import EVENTS_CACHE_DIR, CATALOG_DIR, HLTV_BASE_URL, HLTV_MVP_EVENTS_URL
from src.models import MVPEvent
from src.utils.client import StealthHLTVClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cs2_pipeline.harvest_events")


def parse_mvp_events_html(html_content: str) -> list[MVPEvent]:
    """Parse the HLTV stats/events HTML page into structured MVPEvent objects."""
    soup = BeautifulSoup(html_content, "html.parser")
    events = []

    table = soup.find("table")
    if not table:
        logger.error("No table found in HTML content!")
        return events

    rows = table.find_all("tr")[1:]  # Skip header row

    for row in rows:
        name_col = row.find("td", class_="name-col")
        if not name_col:
            continue

        a_tag = name_col.find("a")
        if not a_tag:
            continue

        href = a_tag.get("href", "")
        m = re.search(r"event=(\d+)", href)
        if not m:
            continue

        event_id = int(m.group(1))
        event_name = a_tag.text.strip()

        winner_col = row.find("td", class_="winner-col")
        winner = None
        if winner_col and winner_col.find("img"):
            winner = winner_col.find("img").get("title")

        maps_col = row.find("td", class_="maps-col")
        maps_count = None
        if maps_col and maps_col.text.strip().isdigit():
            maps_count = int(maps_col.text.strip())

        event = MVPEvent(
            event_id=event_id,
            name=event_name,
            winner=winner,
            maps_count=maps_count,
            results_url=f"{HLTV_BASE_URL}/results?event={event_id}",
        )
        events.append(event)

    return events


def harvest_mvp_events(force_refresh: bool = False) -> list[MVPEvent]:
    """Harvest all pages of CS2 MVP events and order chronologically (oldest CS2 event first)."""
    client = StealthHLTVClient()
    all_events: list[MVPEvent] = []

    try:
        offset = 0
        while True:
            cache_file = EVENTS_CACHE_DIR / f"mvp_events_index_offset{offset}.html"
            if force_refresh and cache_file.exists():
                cache_file.unlink()

            url = f"{HLTV_MVP_EVENTS_URL}&offset={offset}" if offset > 0 else HLTV_MVP_EVENTS_URL

            logger.info("Fetching events page at offset=%d...", offset)
            html = client.get(url, cache_path=cache_file)
            page_events = parse_mvp_events_html(html)

            if not page_events:
                break

            all_events.extend(page_events)
            logger.info("Page offset=%d yielded %d events (total so far: %d)", offset, len(page_events), len(all_events))

            # HLTV displays 50 per page
            if len(page_events) < 50:
                break

            offset += 50

        # Reverse so oldest CS2 event (IEM Sydney 2023 - event 6865) is #1
        all_events.reverse()

        # Persist extracted events list as JSON
        output_file = CATALOG_DIR / "mvp_events.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump([e.to_dict() for e in all_events], f, indent=2)

        logger.info("[COMPLETE] Complete CS2 MVP catalog created: %d events starting from %s (ID %d)",
                    len(all_events), all_events[0].name, all_events[0].event_id)

        return all_events

    finally:
        client.close()


if __name__ == "__main__":
    harvest_mvp_events()
