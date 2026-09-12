"""Rolling worker pipeline for autonomous CS2 match demo ingestion and processing.

Orchestrates the end-to-end lifecycle:
1. Fetch demo targets from SQLite catalog queue.
2. Stream .rar archives from HLTV Cloudflare R2 storage.
3. Unpack .dem Source 2 demo files into temporary scratch storage.
4. Extract tactical combat telemetry and write Snappy Parquet tables to the data lake.
5. Auto-purge intermediate .rar and .dem files to maintain minimal peak disk usage.
6. Record state transitions and performance metrics in the database.
"""
import argparse
import json
import logging
import random
import shutil
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import (
    ARCHIVES_DIR,
    DATA_DIR,
    DEMOS_DIR,
    LAKE_DIR,
    MAX_REQUEST_DELAY_SECONDS,
    MIN_REQUEST_DELAY_SECONDS,
)
from src.db import get_db, init_db
from src.downloader import download_demo_archive, extract_demo_archive
from src.parser import parse_demo_to_lake
from src.utils.client import StealthHLTVClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cs2_pipeline.pipeline")

# Safety threshold: abort if free disk space falls below 2.0 GB
MIN_FREE_DISK_BYTES = 2 * 1024 * 1024 * 1024


def check_disk_space(target_path: Path = DATA_DIR) -> float:
    """Return free disk space in GB and raise error if below safety threshold."""
    usage = shutil.disk_usage(target_path)
    free_gb = usage.free / (1024 * 1024 * 1024)
    if usage.free < MIN_FREE_DISK_BYTES:
        raise RuntimeError(
            f"Disk space critical: only {free_gb:.2f} GB free on {target_path}. "
            f"Halting pipeline to protect host system."
        )
    return free_gb


def get_queue_summary() -> Dict[str, int]:
    """Return summary of demo statuses in the catalog database."""
    with get_db() as conn:
        rows = conn.execute("""
            SELECT status, COUNT(*) as cnt
            FROM demos
            GROUP BY status
        """).fetchall()
    return {row["status"]: row["cnt"] for row in rows}


def fetch_pending_demos(
    event_id: Optional[int] = None,
    demo_id: Optional[int] = None,
    match_id: Optional[int] = None,
    include_failed: bool = False,
    force: bool = False,
    limit: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Query demo targets from SQLite database matching filter criteria."""
    query = """
        SELECT d.demo_id, d.match_id, d.download_url, d.status, d.archive_path,
               d.extracted_paths_json, d.maps_json,
               m.event_id, m.team1, m.team2, m.format, m.match_date,
               e.name as event_name
        FROM demos d
        JOIN matches m ON d.match_id = m.match_id
        JOIN events e ON m.event_id = e.event_id
        WHERE 1=1
    """
    params: List[Any] = []

    if demo_id:
        query += " AND d.demo_id = ?"
        params.append(demo_id)
    elif match_id:
        query += " AND d.match_id = ?"
        params.append(match_id)
    elif event_id:
        query += " AND m.event_id = ?"
        params.append(event_id)

    if not force and not demo_id and not match_id:
        if include_failed:
            query += " AND d.status IN ('DISCOVERED', 'DOWNLOADING', 'DOWNLOADED', 'EXTRACTED', 'FAILED')"
        else:
            query += " AND d.status IN ('DISCOVERED', 'DOWNLOADING', 'DOWNLOADED', 'EXTRACTED')"

    query += " ORDER BY m.event_id ASC, d.match_id ASC"

    if limit:
        query += f" LIMIT {int(limit)}"

    with get_db() as conn:
        rows = conn.execute(query, params).fetchall()
        return [dict(row) for row in rows]


def process_single_demo(
    demo: Dict[str, Any],
    client: StealthHLTVClient,
    keep_demos: bool = False,
    keep_archives: bool = False,
) -> Dict[str, Any]:
    """Execute download, extraction, parsing, and rolling cleanup for one demo target."""
    demo_id = demo["demo_id"]
    match_id = demo["match_id"]
    event_id = demo["event_id"]
    team1 = demo["team1"]
    team2 = demo["team2"]

    free_gb = check_disk_space()
    logger.info(
        "=== Processing Demo %d: Match %d (%s vs %s) [Disk Free: %.1f GB] ===",
        demo_id,
        match_id,
        team1,
        team2,
        free_gb,
    )

    download_occurred = False
    archive_path: Optional[Path] = None
    dem_files: List[Path] = []
    parsed_results: List[Dict[str, Any]] = []

    try:
        # Step 1: Download archive if not already available
        existing_archive = Path(demo["archive_path"]) if demo.get("archive_path") else None
        if existing_archive and existing_archive.exists() and existing_archive.stat().st_size > 0:
            archive_path = existing_archive
            logger.info("Using existing archive: %s", archive_path.name)
        else:
            archive_path = download_demo_archive(demo_id, client=client)
            download_occurred = True

        # Step 2: Unpack archive
        dem_files = extract_demo_archive(
            demo_id=demo_id,
            archive_path=archive_path,
            keep_archive=True,  # preserve until parsing succeeds
        )

        if not dem_files:
            raise RuntimeError(f"No .dem files extracted from archive for demo {demo_id}")

        # Step 3: Parse each map to the Parquet data lake
        for df in dem_files:
            logger.info("Parsing map demo: %s", df.name)
            res = parse_demo_to_lake(
                demo_path=df,
                lake_root=LAKE_DIR,
                match_id=match_id,
                event_id=event_id,
            )
            parsed_results.append(res)

        # Step 4: Rolling Cleanup (Garbage Collection)
        if not keep_demos:
            for df in dem_files:
                if df.exists():
                    df.unlink()
                    logger.info("Purged raw demo: %s", df.name)

            # Remove parent match directory if empty
            if dem_files:
                parent_dir = dem_files[0].parent
                try:
                    if parent_dir.exists() and not any(parent_dir.iterdir()):
                        parent_dir.rmdir()
                        logger.info("Removed empty directory: %s", parent_dir.name)
                except Exception as ex:
                    logger.debug("Could not remove parent dir %s: %s", parent_dir, ex)

        if not keep_archives and archive_path and archive_path.exists():
            archive_path.unlink()
            logger.info("Purged raw archive: %s", archive_path.name)

        # Step 5: Update database status to PARSED
        with get_db() as conn:
            conn.execute(
                """
                UPDATE demos
                SET status = 'PARSED',
                    archive_path = ?,
                    extracted_paths_json = ?,
                    error_message = NULL,
                    updated_at = CURRENT_TIMESTAMP
                WHERE demo_id = ?
                """,
                (
                    str(archive_path) if keep_archives and archive_path else None,
                    json.dumps([str(p) for p in dem_files]) if keep_demos else None,
                    demo_id,
                ),
            )
            conn.commit()

        logger.info(
            "[SUCCESS] Demo %d parsed into %d map lake partitions.",
            demo_id,
            len(parsed_results),
        )

        # Step 6: Apply polite jitter if network download occurred
        if download_occurred:
            jitter = random.uniform(MIN_REQUEST_DELAY_SECONDS, MAX_REQUEST_DELAY_SECONDS)
            logger.info("Pacing delay: sleeping %.1fs before next target...", jitter)
            time.sleep(jitter)

        return {
            "demo_id": demo_id,
            "match_id": match_id,
            "status": "PARSED",
            "maps_parsed": len(parsed_results),
            "details": parsed_results,
        }

    except Exception as e:
        logger.error("Failed processing demo %d: %s", demo_id, e, exc_info=True)
        # Disk safety: purge intermediate scratch files on error to prevent host disk bloat
        if not keep_demos and dem_files:
            for df in dem_files:
                try:
                    if df.exists():
                        df.unlink()
                        logger.info("Purged failed raw demo: %s", df.name)
                except Exception:
                    pass
        if not keep_archives and archive_path:
            try:
                if archive_path.exists():
                    archive_path.unlink()
                    logger.info("Purged failed raw archive: %s", archive_path.name)
            except Exception:
                pass

        with get_db() as conn:
            conn.execute(
                """
                UPDATE demos
                SET status = 'FAILED',
                    error_message = ?,
                    updated_at = CURRENT_TIMESTAMP
                WHERE demo_id = ?
                """,
                (str(e), demo_id),
            )
            conn.commit()
        raise


def run_pipeline(
    event_id: Optional[int] = None,
    demo_id: Optional[int] = None,
    match_id: Optional[int] = None,
    limit: Optional[int] = None,
    keep_demos: bool = False,
    keep_archives: bool = False,
    force: bool = False,
    dry_run: bool = False,
) -> Dict[str, Any]:
    """Execute the automated rolling worker pipeline over pending matches."""
    init_db()
    targets = fetch_pending_demos(
        event_id=event_id,
        demo_id=demo_id,
        match_id=match_id,
        force=force,
        limit=limit,
    )

    logger.info("Found %d demo targets matching criteria.", len(targets))

    if dry_run:
        logger.info("--- Dry Run Target List ---")
        for t in targets:
            logger.info(
                "  Demo %d: Match %d (%s vs %s) - Status: %s",
                t["demo_id"],
                t["match_id"],
                t["team1"],
                t["team2"],
                t["status"],
            )
        return {"total_targets": len(targets), "dry_run": True}

    if not targets:
        logger.info("No pending demos to process.")
        return {"processed": 0, "success": 0, "failed": 0}

    client = StealthHLTVClient()
    success_count = 0
    failed_count = 0

    try:
        for idx, target in enumerate(targets, 1):
            logger.info("Processing target %d of %d...", idx, len(targets))
            try:
                process_single_demo(
                    demo=target,
                    client=client,
                    keep_demos=keep_demos,
                    keep_archives=keep_archives,
                )
                success_count += 1
            except Exception as e:
                failed_count += 1
                logger.error("Skipping demo %d due to error: %s", target["demo_id"], e)
                # Halt if Turnstile or Cloudflare challenge detected
                if "challenge" in str(e).lower() or "cloudflare" in str(e).lower():
                    logger.critical("Cloudflare challenge encountered. Aborting pipeline run.")
                    break
    finally:
        client.close()

    summary = {
        "processed": success_count + failed_count,
        "success": success_count,
        "failed": failed_count,
        "queue_status": get_queue_summary(),
    }
    logger.info("Pipeline run finished: %s", summary)
    return summary


def display_status() -> None:
    """Print catalog and pipeline queue health."""
    init_db()
    summary = get_queue_summary()
    print("\n=== CS2 Demo Pipeline Queue Status ===")
    if not summary:
        print("No demo entries found in database catalog.")
        return

    total = sum(summary.values())
    for status, count in sorted(summary.items()):
        pct = (count / total) * 100
        print(f"  {status:<15}: {count:>5} ({pct:>5.1f}%)")
    print(f"  {'TOTAL':<15}: {total:>5} (100.0%)\n")

    free_gb = check_disk_space()
    print(f"Host Disk Free Space: {free_gb:.2f} GB\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Autonomous rolling worker pipeline for CS2 demos and Parquet lake generation."
    )
    parser.add_argument("--event", type=int, default=None, help="Filter by HLTV event ID (e.g. 6865)")
    parser.add_argument("--demo", type=int, default=None, help="Process a single demo ID")
    parser.add_argument("--match", type=int, default=None, help="Process a single match ID")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of demos to process")
    parser.add_argument("--keep-demos", action="store_true", help="Keep raw .dem files instead of auto-purging")
    parser.add_argument("--keep-archives", action="store_true", help="Keep raw .rar archives instead of auto-purging")
    parser.add_argument("--force", action="store_true", help="Reprocess even if already marked PARSED")
    parser.add_argument("--dry-run", action="store_true", help="Display queue without processing")
    parser.add_argument("--status", action="store_true", help="Display database queue status summary")

    args = parser.parse_args()

    if args.status:
        display_status()
    else:
        run_pipeline(
            event_id=args.event,
            demo_id=args.demo,
            match_id=args.match,
            limit=args.limit,
            keep_demos=args.keep_demos,
            keep_archives=args.keep_archives,
            force=args.force,
            dry_run=args.dry_run,
        )
