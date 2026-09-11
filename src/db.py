"""SQLite database catalog for CS2 professional match demos."""
import json
import sqlite3
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import DATABASE_PATH


def get_db(db_path: Path = DATABASE_PATH) -> sqlite3.Connection:
    """Connect to SQLite database with foreign keys enabled."""
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DATABASE_PATH) -> None:
    """Initialize database tables if they do not exist."""
    with get_db(db_path) as conn:
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS events (
            event_id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            winner TEXT,
            maps_count INTEGER,
            results_url TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS matches (
            match_id INTEGER PRIMARY KEY,
            event_id INTEGER NOT NULL,
            team1 TEXT NOT NULL,
            team2 TEXT NOT NULL,
            score1 INTEGER,
            score2 INTEGER,
            format TEXT,
            url TEXT NOT NULL,
            match_date TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (event_id) REFERENCES events (event_id)
        );

        CREATE TABLE IF NOT EXISTS demos (
            demo_id INTEGER PRIMARY KEY,
            match_id INTEGER UNIQUE NOT NULL,
            download_url TEXT NOT NULL,
            maps_json TEXT,
            status TEXT DEFAULT 'DISCOVERED',  -- DISCOVERED, DOWNLOADING, DOWNLOADED, EXTRACTED, FAILED
            archive_path TEXT,
            extracted_paths_json TEXT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches (match_id)
        );

        CREATE INDEX IF NOT EXISTS idx_matches_event ON matches(event_id);
        CREATE INDEX IF NOT EXISTS idx_demos_status ON demos(status);
        """)


def upsert_event(conn: sqlite3.Connection, event: Dict[str, Any]) -> None:
    """Insert or update an MVP event."""
    conn.execute(
        """
        INSERT INTO events (event_id, name, winner, maps_count, results_url)
        VALUES (:event_id, :name, :winner, :maps_count, :results_url)
        ON CONFLICT(event_id) DO UPDATE SET
            name=excluded.name,
            winner=excluded.winner,
            maps_count=excluded.maps_count,
            results_url=excluded.results_url;
        """,
        event,
    )


def upsert_match(conn: sqlite3.Connection, match: Dict[str, Any]) -> None:
    """Insert or update a match."""
    conn.execute(
        """
        INSERT INTO matches (match_id, event_id, team1, team2, score1, score2, format, url, match_date)
        VALUES (:match_id, :event_id, :team1, :team2, :score1, :score2, :format, :url, :match_date)
        ON CONFLICT(match_id) DO UPDATE SET
            team1=excluded.team1,
            team2=excluded.team2,
            score1=excluded.score1,
            score2=excluded.score2,
            format=excluded.format,
            url=excluded.url,
            match_date=excluded.match_date;
        """,
        match,
    )


def upsert_demo(conn: sqlite3.Connection, demo: Dict[str, Any]) -> None:
    """Insert or update a demo download target."""
    maps_json = json.dumps(demo.get("maps", []))
    conn.execute(
        """
        INSERT INTO demos (demo_id, match_id, download_url, maps_json, status)
        VALUES (?, ?, ?, ?, 'DISCOVERED')
        ON CONFLICT(demo_id) DO UPDATE SET
            download_url=excluded.download_url,
            maps_json=excluded.maps_json;
        """,
        (demo["demo_id"], demo["match_id"], demo["download_url"], maps_json),
    )
