"""Upload CS2 catalog data from SQLite to Supabase PostgreSQL."""
import json
import math
import os
import sqlite3
import sys
import time
from pathlib import Path
from typing import Any, Dict, List
from urllib import error, request

from src.config import (
    DATABASE_PATH,
    SUPABASE_SERVICE_ROLE_KEY,
    SUPABASE_URL,
)

SERVICE_ROLE_KEY = SUPABASE_SERVICE_ROLE_KEY
BATCH_SIZE = 500


def clean_val(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
        return None
    return v


def upsert_batch(table: str, on_conflict: str, batch: List[Dict[str, Any]]) -> None:
    endpoint = f"{SUPABASE_URL.rstrip('/')}/rest/v1/{table}?on_conflict={on_conflict}"
    body = json.dumps(batch).encode("utf-8")
    req = request.Request(
        endpoint,
        data=body,
        method="POST",
        headers={
            "Content-Type": "application/json",
            "apikey": SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SERVICE_ROLE_KEY}",
            "Prefer": "resolution=merge-duplicates,return=minimal",
        },
    )
    for attempt in range(3):
        try:
            with request.urlopen(req, timeout=60):
                return
        except error.HTTPError as e:
            err_msg = e.read().decode("utf-8", errors="replace")
            if attempt == 2:
                raise RuntimeError(f"Failed upsert to {table} (HTTP {e.code}): {err_msg}")
            time.sleep(2 * (attempt + 1))


def upload_table(table_name: str, on_conflict: str, rows: List[Dict[str, Any]]):
    total = len(rows)
    print(f"Uploading {total:,} rows to {table_name}...")
    for i in range(0, total, BATCH_SIZE):
        batch = rows[i : i + BATCH_SIZE]
        upsert_batch(table_name, on_conflict, batch)
        print(f"  [{table_name}] uploaded rows {i + 1} - {min(i + BATCH_SIZE, total)} / {total}")
    print(f"Done uploading {table_name}.\n")


def main():
    if not SERVICE_ROLE_KEY:
        print("Error: SUPABASE_SERVICE_ROLE_KEY is required.")
        print("Please set SUPABASE_SERVICE_ROLE_KEY in your .env.local file.")
        sys.exit(1)

    if not os.path.exists(DATABASE_PATH):
        print(f"Database not found at {DATABASE_PATH}")
        sys.exit(1)

    conn = sqlite3.connect(DATABASE_PATH)
    conn.row_factory = sqlite3.Row

    # 1. Events
    print("Preparing cs2_events...")
    event_rows = [
        {
            "event_id": r["event_id"],
            "name": r["name"],
            "winner": r["winner"],
            "maps_count": r["maps_count"],
            "results_url": r["results_url"],
            "created_at": r["created_at"],
        }
        for r in conn.execute("SELECT * FROM events").fetchall()
    ]
    upload_table("cs2_events", "event_id", event_rows)

    # 2. Matches
    print("Preparing cs2_matches...")
    match_rows = [
        {
            "match_id": r["match_id"],
            "event_id": r["event_id"],
            "team1": r["team1"],
            "team2": r["team2"],
            "score1": clean_val(r["score1"]),
            "score2": clean_val(r["score2"]),
            "format": r["format"],
            "url": r["url"],
            "match_date": r["match_date"],
            "created_at": r["created_at"],
        }
        for r in conn.execute("SELECT * FROM matches").fetchall()
    ]
    upload_table("cs2_matches", "match_id", match_rows)

    # 3. Demos
    print("Preparing cs2_demos...")
    demo_rows = []
    for r in conn.execute("SELECT * FROM demos").fetchall():
        maps_json = json.loads(r["maps_json"]) if r["maps_json"] else None
        extracted_paths = json.loads(r["extracted_paths_json"]) if r["extracted_paths_json"] else None
        demo_rows.append({
            "demo_id": r["demo_id"],
            "match_id": r["match_id"],
            "download_url": r["download_url"],
            "maps_json": maps_json,
            "status": r["status"],
            "archive_path": r["archive_path"],
            "extracted_paths_json": extracted_paths,
            "error_message": r["error_message"],
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        })
    upload_table("cs2_demos", "demo_id", demo_rows)

    # 4. Map Stats
    print("Preparing cs2_map_stats...")
    map_stat_rows = [
        {
            "match_id": r["match_id"],
            "event_id": r["event_id"],
            "map_name": r["map_name"],
            "team1": r["team1"],
            "team2": r["team2"],
            "score1": r["score1"],
            "score2": r["score2"],
            "half_scores": r["half_scores"],
            "created_at": r["created_at"],
        }
        for r in conn.execute("SELECT * FROM hltv_map_stats").fetchall()
    ]
    upload_table("cs2_map_stats", "match_id,map_name", map_stat_rows)

    # 5. Player Stats
    print("Preparing cs2_player_stats...")
    player_stat_rows = [
        {
            "match_id": r["match_id"],
            "event_id": r["event_id"],
            "map_name": r["map_name"],
            "team": r["team"],
            "player_id": clean_val(r["player_id"]),
            "player_nick": r["player_nick"],
            "kills": r["kills"],
            "deaths": r["deaths"],
            "plus_minus": clean_val(r["plus_minus"]),
            "adr": clean_val(r["adr"]),
            "kast_pct": clean_val(r["kast_pct"]),
            "rating": clean_val(r["rating"]),
            "round_swing": clean_val(r["round_swing"]),
            "created_at": r["created_at"],
        }
        for r in conn.execute("SELECT * FROM hltv_player_stats").fetchall()
    ]
    upload_table("cs2_player_stats", "match_id,map_name,player_id", player_stat_rows)

    conn.close()
    print("All CS2 tables successfully uploaded to Supabase!")


if __name__ == "__main__":
    main()
