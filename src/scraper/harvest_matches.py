"""Harvest matches and demo download links for a target event with cautious rate limiting and caching."""
import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

from src.config import (
    EVENTS_CACHE_DIR,
    MATCHES_CACHE_DIR,
    CATALOG_DIR,
    HLTV_BASE_URL,
)
from src.db import get_db, upsert_match, upsert_demo
from src.models import Match
from src.utils.client import StealthHLTVClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cs2_pipeline.harvest_matches")


def parse_event_matches_html(html_content: str, event_id: int, event_name: str = "") -> List[Dict[str, Any]]:
    """Extract list of matches from an event results page."""
    soup = BeautifulSoup(html_content, "html.parser")
    result_divs = soup.find_all("div", class_="result-con")
    matches = []

    for r in result_divs:
        a_tag = r.find("a", class_="a-reset") or r.find("a")
        if not a_tag:
            continue

        href = a_tag.get("href", "")
        match_search = re.search(r"/matches/(\d+)/", href)
        if not match_search:
            continue

        match_id = int(match_search.group(1))
        match_url = f"{HLTV_BASE_URL}{href}"

        # Team names
        team1_elem = r.find("div", class_="team1")
        team2_elem = r.find("div", class_="team2")

        team1_name = team1_elem.find("div", class_="team").text.strip() if team1_elem and team1_elem.find("div", class_="team") else "Unknown"
        team2_name = team2_elem.find("div", class_="team").text.strip() if team2_elem and team2_elem.find("div", class_="team") else "Unknown"

        # Scores
        score_won = r.find("span", class_="score-won")
        score_lost = r.find("span", class_="score-lost")
        score1 = int(score_lost.text.strip()) if score_lost else 0
        score2 = int(score_won.text.strip()) if score_won else 0

        # Format (bo1, bo3, bo5)
        format_elem = r.find("div", class_="map-text")
        match_format = format_elem.text.strip().lower() if format_elem else "bo3"

        # Date / timestamp
        unix_ts = r.get("data-zonedgrouping-entry-unix")
        match_date = None
        if unix_ts and unix_ts.isdigit():
            match_date = datetime.utcfromtimestamp(int(unix_ts) / 1000.0).strftime("%Y-%m-%d %H:%M:%S")

        matches.append({
            "match_id": match_id,
            "event_id": event_id,
            "team1": team1_name,
            "team2": team2_name,
            "score1": score1,
            "score2": score2,
            "format": match_format,
            "url": match_url,
            "match_date": match_date,
        })

    logger.info("Found %d matches for event %d", len(matches), event_id)
    return matches


def parse_match_page_demos(html_content: str, match_id: int) -> Optional[Dict[str, Any]]:
    """Extract demo download ID and maps played from a match page."""
    soup = BeautifulSoup(html_content, "html.parser")

    # Find demo link
    demo_tag = soup.find("a", href=re.compile(r"/download/demo/(\d+)"))
    if not demo_tag:
        logger.warning("No GOTV demo link found for match %d", match_id)
        return None

    href = demo_tag["href"]
    m = re.search(r"/download/demo/(\d+)", href)
    if not m:
        return None

    demo_id = int(m.group(1))
    download_url = f"{HLTV_BASE_URL}/download/demo/{demo_id}"

    # Extract maps played
    maps = []
    for mh in soup.find_all("div", class_="mapholder"):
        name_elem = mh.find("div", class_="mapname")
        if name_elem:
            maps.append(name_elem.text.strip().lower())

    return {
        "demo_id": demo_id,
        "match_id": match_id,
        "download_url": download_url,
        "maps": maps,
    }


def harvest_matches_for_event(event_id: int, resolve_demos: bool = True, max_matches: Optional[int] = None) -> List[Dict[str, Any]]:
    """Crawl results page and resolve demo links for an event."""
    client = StealthHLTVClient()
    event_cache = EVENTS_CACHE_DIR / f"event_{event_id}_results.html"
    results_url = f"{HLTV_BASE_URL}/results?event={event_id}"

    try:
        logger.info("Fetching results for event %d...", event_id)
        html = client.get(results_url, cache_path=event_cache)
        matches = parse_event_matches_html(html, event_id=event_id)

        # Upsert matches to DB
        with get_db() as conn:
            for m in matches:
                upsert_match(conn, m)
            conn.commit()

        if not resolve_demos:
            return matches

        # Fetch existing demos from DB to avoid duplicate work
        with get_db() as conn:
            existing_match_demos = {row["match_id"] for row in conn.execute("SELECT match_id FROM demos")}

        target_matches = matches[:max_matches] if max_matches else matches
        logger.info("Resolving demo links for %d matches (%d already in DB)...",
                    len(target_matches), len(existing_match_demos))

        resolved_count = 0
        for idx, m in enumerate(target_matches, 1):
            match_id = m["match_id"]
            if match_id in existing_match_demos:
                logger.info("[%d/%d] Match %d (%s vs %s) already in DB. Skipping.",
                            idx, len(target_matches), match_id, m["team1"], m["team2"])
                continue

            match_url = m["url"]
            match_cache = MATCHES_CACHE_DIR / f"match_{match_id}.html"

            logger.info("[%d/%d] Fetching match %d (%s vs %s)...",
                        idx, len(target_matches), match_id, m["team1"], m["team2"])
            match_html = client.get(match_url, referer=results_url, cache_path=match_cache)
            demo_info = parse_match_page_demos(match_html, match_id)

            if demo_info:
                with get_db() as conn:
                    upsert_demo(conn, demo_info)
                    conn.commit()
                resolved_count += 1
                logger.info("[RESOLVED] Demo %d resolved with maps: %s", demo_info["demo_id"], demo_info["maps"])

        logger.info("[COMPLETE] Completed harvesting for event %d! (Added %d new demos)", event_id, resolved_count)
        return target_matches

    finally:
        client.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Harvest matches and demos for an HLTV event.")
    parser.add_argument("--event", type=int, default=6865, help="HLTV Event ID (default: 6865 for IEM Sydney 2023)")
    parser.add_argument("--max-matches", type=int, default=None, help="Limit number of match pages to crawl")
    args = parser.parse_args()

    harvest_matches_for_event(event_id=args.event, max_matches=args.max_matches)
