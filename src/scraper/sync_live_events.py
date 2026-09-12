"""Automated and incremental synchronizer for CS2 MVP events, matches, and demos.

Continuously keeps the CS2 catalog updated with new matches and tournaments from:
https://www.hltv.org/stats/events?csVersion=CS2&matchType=MvpEvents

Features:
- Discovers newly scheduled or concluded CS2 MVP tournaments.
- Detects map count increases or missing winners in ongoing tournaments.
- Re-crawls event results pages (bypassing stale caches) to discover new matches.
- Resolves GOTV demo download targets (queued as DISCOVERED) and HLTV player stats.
- Concurrency-safe: Runs in SQLite WAL mode alongside active demo processing pipelines.
- Supports both one-shot CLI execution and continuous background daemon (--watch).
"""
import argparse
import logging
import sys
import time
from typing import Any, Dict, List, Optional, Set

from src.config import HLTV_MVP_EVENTS_URL
from src.db import get_db, init_db, upsert_event
from src.pipeline import run_pipeline
from src.scraper.harvest_events import harvest_mvp_events
from src.scraper.harvest_matches import harvest_matches_for_event
from src.utils.client import StealthHLTVClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cs2_pipeline.sync_live_events")


def get_stored_events_summary() -> Dict[int, Dict[str, Any]]:
    """Retrieve existing events from SQLite with map counts and match counts."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT e.event_id, e.name, e.winner, e.maps_count,
                   COUNT(m.match_id) as matches_in_db,
                   SUM(CASE WHEN d.status = 'PARSED' THEN 1 ELSE 0 END) as parsed_demos,
                   SUM(CASE WHEN d.status = 'DISCOVERED' THEN 1 ELSE 0 END) as discovered_demos
            FROM events e
            LEFT JOIN matches m ON e.event_id = m.event_id
            LEFT JOIN demos d ON m.match_id = d.match_id
            GROUP BY e.event_id
        """).fetchall()
        return {r["event_id"]: dict(r) for r in rows}


def sync_mvp_events(
    client: StealthHLTVClient,
    force_all: bool = False,
    specific_event: Optional[int] = None,
    auto_parse: bool = False,
) -> Dict[str, Any]:
    """Inspect HLTV MVP events index and synchronize any newly played matches/events."""
    init_db()
    existing_events = get_stored_events_summary()
    logger.info("Current catalog contains %d events in database.", len(existing_events))

    # Step 1: Freshly harvest MVP events from HLTV
    logger.info("Checking %s for latest events...", HLTV_MVP_EVENTS_URL)
    latest_events = harvest_mvp_events(force_refresh=True, client=client)

    events_to_sync: List[Dict[str, Any]] = []
    new_events_count = 0
    updated_events_count = 0

    for ev in latest_events:
        eid = ev.event_id
        ev_dict = ev.to_dict()

        if specific_event and eid != specific_event:
            continue

        # Policy: Process only concluded tournaments (winner must be officially crowned)
        has_winner = bool(ev.winner and ev.winner.strip() and ev.winner.strip().lower() != "unknown")
        if not has_winner and not force_all:
            logger.info("[SKIPPED - ONGOING] Tournament %d (%s) is still in progress (no winner crowned yet).", eid, ev.name)
            continue

        stored = existing_events.get(eid)
        if not stored:
            logger.info("[NEW CONCLUDED EVENT] Discovered newly concluded tournament: %d - %s (Winner: %s)", eid, ev.name, ev.winner)
            events_to_sync.append(ev_dict)
            new_events_count += 1
            continue

        # If stored event previously had no winner, but now has a winner -> Newly concluded!
        stored_winner = stored.get("winner")
        stored_had_no_winner = not stored_winner or stored_winner == "Unknown"

        # Check if matches in database are missing or need syncing
        matches_in_db = stored.get("matches_in_db") or 0
        stored_maps = stored.get("maps_count") or 0
        latest_maps = ev.maps_count or 0

        if force_all or stored_had_no_winner or (matches_in_db == 0) or (latest_maps > stored_maps):
            reasons = []
            if force_all:
                reasons.append("force_all")
            if stored_had_no_winner:
                reasons.append(f"event concluded (winner crowned: {ev.winner})")
            if matches_in_db == 0:
                reasons.append("no matches cataloged yet")
            if latest_maps > stored_maps:
                reasons.append(f"final maps count updated: {stored_maps} -> {latest_maps}")

            logger.info("[SYNC CONCLUDED TOURNAMENT] %d (%s, Winner: %s): %s", eid, ev.name, ev.winner, ", ".join(reasons))
            events_to_sync.append(ev_dict)
            updated_events_count += 1

    logger.info(
        "Scan complete: %d events need match synchronization (%d new, %d updated).",
        len(events_to_sync),
        new_events_count,
        updated_events_count,
    )

    if not events_to_sync:
        logger.info("All MVP tournaments and matches are up to date.")
        return {
            "new_events": 0,
            "updated_events": 0,
            "new_matches": 0,
            "new_demos": 0,
        }

    # Step 2: Synchronize matches for target events
    total_new_matches = 0
    total_new_demos = 0

    for idx, ev in enumerate(events_to_sync, 1):
        eid = ev["event_id"]
        ename = ev["name"]
        logger.info("--- Syncing Matches [%d/%d]: %d - %s ---", idx, len(events_to_sync), eid, ename)

        with get_db() as conn:
            prior_match_ids: Set[int] = {
                r["match_id"] for r in conn.execute("SELECT match_id FROM matches WHERE event_id = ?", (eid,))
            }
            prior_demo_ids: Set[int] = {
                r["demo_id"] for r in conn.execute(
                    "SELECT d.demo_id FROM demos d JOIN matches m ON d.match_id = m.match_id WHERE m.event_id = ?",
                    (eid,),
                )
            }

        # Force refresh results page so newly finished matches are visible
        harvest_matches_for_event(
            event_id=eid,
            resolve_demos=True,
            client=client,
            force_refresh_results=True,
        )

        with get_db() as conn:
            after_match_ids: Set[int] = {
                r["match_id"] for r in conn.execute("SELECT match_id FROM matches WHERE event_id = ?", (eid,))
            }
            after_demo_ids: Set[int] = {
                r["demo_id"] for r in conn.execute(
                    "SELECT d.demo_id FROM demos d JOIN matches m ON d.match_id = m.match_id WHERE m.event_id = ?",
                    (eid,),
                )
            }

        new_m = len(after_match_ids - prior_match_ids)
        new_d = len(after_demo_ids - prior_demo_ids)
        total_new_matches += new_m
        total_new_demos += new_d

        logger.info("Synced %s: +%d new matches, +%d new GOTV demos cataloged.", ename, new_m, new_d)

        # Optional: immediately parse newly discovered demos into Parquet lake
        if auto_parse and new_d > 0:
            logger.info("Auto-parsing %d new demos for %s into Parquet lake...", new_d, ename)
            run_pipeline(event_id=eid, keep_demos=False, keep_archives=False)

    logger.info(
        "[SYNC FINISHED] Total: +%d new events, +%d updated events, +%d new matches, +%d new demos.",
        new_events_count,
        updated_events_count,
        total_new_matches,
        total_new_demos,
    )

    return {
        "new_events": new_events_count,
        "updated_events": updated_events_count,
        "new_matches": total_new_matches,
        "new_demos": total_new_demos,
    }


def run_sync_daemon(interval_seconds: int = 1800, auto_parse: bool = False) -> None:
    """Continuously monitor and sync new matches at a fixed interval."""
    logger.info("Starting automated sync daemon (interval: %d seconds, auto_parse=%s)...",
                interval_seconds, auto_parse)
    client = StealthHLTVClient()
    try:
        iteration = 1
        while True:
            logger.info("=== Sync Iteration #%d ===", iteration)
            try:
                sync_mvp_events(client=client, auto_parse=auto_parse)
            except Exception as e:
                logger.error("Error during sync iteration #%d: %s", iteration, e, exc_info=True)

            logger.info("Sleeping %d seconds until next sync check...", interval_seconds)
            time.sleep(interval_seconds)
            iteration += 1
    finally:
        client.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Automated synchronizer for new CS2 MVP events, matches, and GOTV demos."
    )
    parser.add_argument("--event", type=int, default=None, help="Sync a specific event ID only")
    parser.add_argument("--all", action="store_true", help="Force sync check across all tournaments")
    parser.add_argument("--parse", action="store_true", help="Automatically trigger demo parsing for new matches")
    parser.add_argument("--watch", action="store_true", help="Run in continuous daemon mode")
    parser.add_argument("--interval", type=int, default=1800, help="Watch mode interval in seconds (default: 1800)")

    args = parser.parse_args()

    if args.watch:
        run_sync_daemon(interval_seconds=args.interval, auto_parse=args.parse)
    else:
        client = StealthHLTVClient()
        try:
            sync_mvp_events(
                client=client,
                force_all=args.all,
                specific_event=args.event,
                auto_parse=args.parse,
            )
        finally:
            client.close()
