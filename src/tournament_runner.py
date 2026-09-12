"""Master tournament runner for end-to-end processing across all CS2 MVP events.

Coordinates the complete lifecycle per event in chronological order:
1. Harvest matches, GOTV demo URLs, and official HLTV boxscores with Round Swing.
2. Ingest, unpack, parse, and write Snappy Parquet lake tables via the rolling worker.
3. Auto-purge raw .rar and .dem files to ensure minimal peak disk usage.
4. Checkpoint state after every match, enabling seamless pause/resume.
"""
import argparse
import json
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import (
    CATALOG_DIR,
    DATA_DIR,
    LAKE_DIR,
)
from src.db import get_db, init_db
from src.pipeline import check_disk_space, run_pipeline
from src.scraper.harvest_matches import harvest_matches_for_event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cs2_pipeline.tournament_runner")


def load_chronological_events() -> List[Dict[str, Any]]:
    """Load all MVP events in chronological order from mvp_events.json or database."""
    catalog_json = CATALOG_DIR / "mvp_events.json"
    if catalog_json.exists():
        with open(catalog_json, "r", encoding="utf-8") as f:
            return json.load(f)

    with get_db() as conn:
        rows = conn.execute("""
            SELECT event_id, name, winner, maps_count, results_url
            FROM events
            ORDER BY event_id ASC
        """).fetchall()
        return [dict(r) for r in rows]


def get_event_progress(event_id: int) -> Dict[str, Any]:
    """Calculate match and demo progress for a specific event."""
    with get_db() as conn:
        matches = conn.execute(
            "SELECT COUNT(*) as cnt FROM matches WHERE event_id = ?",
            (event_id,),
        ).fetchone()["cnt"]

        demos = conn.execute("""
            SELECT d.status, COUNT(*) as cnt
            FROM demos d
            JOIN matches m ON d.match_id = m.match_id
            WHERE m.event_id = ?
            GROUP BY d.status
        """, (event_id,)).fetchall()

    demo_counts = {r["status"]: r["cnt"] for r in demos}
    total_demos = sum(demo_counts.values())
    parsed_demos = demo_counts.get("PARSED", 0)

    is_complete = total_demos > 0 and parsed_demos == total_demos

    return {
        "event_id": event_id,
        "matches_cataloged": matches,
        "demos_total": total_demos,
        "demos_parsed": parsed_demos,
        "is_complete": is_complete,
        "status_breakdown": demo_counts,
    }


def process_event(event: Dict[str, Any], force: bool = False) -> Dict[str, Any]:
    """Execute match harvesting and demo parsing for a single tournament."""
    event_id = event["event_id"]
    event_name = event["name"]

    check_disk_space()
    progress = get_event_progress(event_id)

    if not force and progress["is_complete"]:
        logger.info(
            "Tournament %d (%s) already 100%% parsed (%d/%d matches). Skipping.",
            event_id,
            event_name,
            progress["demos_parsed"],
            progress["demos_total"],
        )
        return progress

    winner = event.get("winner")
    if not winner or str(winner).strip().lower() in ("none", "unknown", "tbd"):
        logger.info(
            "Tournament %d (%s) has no crowned winner yet (ongoing). Skipping per concluded-only policy.",
            event_id,
            event_name,
        )
        return progress

    logger.info(
        "================================================================================"
    )
    logger.info("STARTING TOURNAMENT: %d - %s", event_id, event_name)
    logger.info(
        "================================================================================"
    )

    # Step 1: Harvest matches, demo targets, and HLTV stats
    logger.info("Phase 1: Harvesting matches and HLTV boxscores for %s...", event_name)
    harvest_matches_for_event(event_id=event_id, resolve_demos=True)

    # Step 2: Run rolling worker pipeline on pending demos
    logger.info("Phase 2: Ingesting and parsing demos into Parquet lake for %s...", event_name)
    pipeline_res = run_pipeline(
        event_id=event_id,
        keep_demos=False,
        keep_archives=False,
        force=force,
    )

    # Step 3: Summarize updated progress
    updated_progress = get_event_progress(event_id)
    logger.info(
        "[TOURNAMENT COMPLETE] %s: %d/%d demos parsed into Parquet lake.",
        event_name,
        updated_progress["demos_parsed"],
        updated_progress["demos_total"],
    )
    return updated_progress


def run_all_tournaments(
    limit_events: Optional[int] = None,
    specific_event: Optional[int] = None,
    force: bool = False,
) -> None:
    """Run pipeline sequentially across tournaments."""
    init_db()
    all_events = load_chronological_events()

    if specific_event:
        targets = [e for e in all_events if e["event_id"] == specific_event]
        if not targets:
            logger.error("Event ID %d not found in catalog.", specific_event)
            return
    else:
        targets = all_events

    if limit_events:
        targets = targets[:limit_events]

    logger.info(
        "Beginning tournament runner for %d tournaments (Free Disk: %.1f GB)...",
        len(targets),
        check_disk_space(),
    )

    for idx, event in enumerate(targets, 1):
        try:
            logger.info("--- Tournament [%d/%d] ---", idx, len(targets))
            process_event(event, force=force)
        except Exception as e:
            logger.error(
                "Error processing tournament %d (%s): %s",
                event["event_id"],
                event["name"],
                e,
                exc_info=True,
            )
            # If Cloudflare block occurs, pause to protect IP
            if "challenge" in str(e).lower() or "cloudflare" in str(e).lower():
                logger.critical("Cloudflare challenge encountered. Halting runner to protect IP.")
                break

    display_global_dashboard()


def display_global_dashboard() -> None:
    """Print global overview of all tournaments, lake size, and parsing status."""
    init_db()
    all_events = load_chronological_events()

    total_tournaments = len(all_events)
    completed_tournaments = 0
    total_parsed_demos = 0
    total_matches = 0

    print("\n" + "=" * 90)
    print(f"{'CS2 MVP TOURNAMENT MASTER PROGRESS DASHBOARD':^90}")
    print("=" * 90)
    print(f"{'ID':<6} {'Tournament Name':<42} {'Winner':<14} {'Matches':<8} {'Parsed':<8} {'Status'}")
    print("-" * 90)

    for e in all_events:
        eid = e["event_id"]
        name = e["name"][:40]
        winner = e.get("winner", "Unknown") or "Unknown"

        prog = get_event_progress(eid)
        m_count = prog["matches_cataloged"]
        p_count = prog["demos_parsed"]
        d_count = prog["demos_total"]

        total_matches += m_count
        total_parsed_demos += p_count

        if prog["is_complete"]:
            completed_tournaments += 1
            status_str = "COMPLETE"
        elif p_count > 0:
            status_str = f"IN PROGRESS ({p_count}/{d_count})"
        elif m_count > 0:
            status_str = f"QUEUED ({m_count} matches)"
        else:
            status_str = "PENDING CRAWL"

        print(f"{eid:<6} {name:<42} {winner:<14} {m_count:<8} {p_count:<8} {status_str}")

    lake_size_mb = 0.0
    if LAKE_DIR.exists():
        lake_bytes = sum(f.stat().st_size for f in LAKE_DIR.rglob("*.parquet"))
        lake_size_mb = lake_bytes / (1024 * 1024)

    free_disk_gb = check_disk_space()

    print("=" * 90)
    print(f"Tournaments Completed : {completed_tournaments} / {total_tournaments} ({(completed_tournaments / total_tournaments) * 100:.1f}%)")
    print(f"Total Matches Parsed  : {total_parsed_demos} / {total_matches}")
    print(f"Total Lake Parquet Size: {lake_size_mb:.2f} MB")
    print(f"Host Free Disk Space  : {free_disk_gb:.2f} GB")
    print("=" * 90 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Master tournament runner across all CS2 MVP events."
    )
    parser.add_argument("--all", action="store_true", help="Process all tournaments consecutively")
    parser.add_argument("--event", type=int, default=None, help="Process a specific tournament event ID")
    parser.add_argument("--limit-events", type=int, default=None, help="Limit number of tournaments to process")
    parser.add_argument("--force", action="store_true", help="Reprocess already completed tournaments")
    parser.add_argument("--status", action="store_true", help="Display global tournament progress dashboard")

    args = parser.parse_args()

    if args.status:
        display_global_dashboard()
    else:
        run_all_tournaments(
            limit_events=args.limit_events,
            specific_event=args.event,
            force=args.force,
        )
