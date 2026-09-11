"""SQLite database catalog for CS2 professional match demos and HLTV stats."""
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
            status TEXT DEFAULT 'DISCOVERED',  -- DISCOVERED, DOWNLOADING, DOWNLOADED, EXTRACTED, PARSED, FAILED
            archive_path TEXT,
            extracted_paths_json TEXT,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches (match_id)
        );

        CREATE TABLE IF NOT EXISTS hltv_player_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            map_name TEXT NOT NULL,
            team TEXT NOT NULL,
            player_id INTEGER,
            player_nick TEXT NOT NULL,
            kills INTEGER NOT NULL,
            deaths INTEGER NOT NULL,
            plus_minus INTEGER,
            adr REAL,
            kast_pct REAL,
            rating REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches (match_id),
            UNIQUE(match_id, map_name, player_id)
        );

        CREATE TABLE IF NOT EXISTS hltv_map_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            match_id INTEGER NOT NULL,
            event_id INTEGER NOT NULL,
            map_name TEXT NOT NULL,
            team1 TEXT NOT NULL,
            team2 TEXT NOT NULL,
            score1 INTEGER NOT NULL,
            score2 INTEGER NOT NULL,
            half_scores TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (match_id) REFERENCES matches (match_id),
            UNIQUE(match_id, map_name)
        );

        CREATE INDEX IF NOT EXISTS idx_matches_event ON matches(event_id);
        CREATE INDEX IF NOT EXISTS idx_demos_status ON demos(status);
        CREATE INDEX IF NOT EXISTS idx_hltv_player_stats_match ON hltv_player_stats(match_id);
        CREATE INDEX IF NOT EXISTS idx_hltv_player_stats_player ON hltv_player_stats(player_id);
        CREATE INDEX IF NOT EXISTS idx_hltv_map_stats_match ON hltv_map_stats(match_id);
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


def upsert_hltv_player_stat(conn: sqlite3.Connection, stat: Dict[str, Any]) -> None:
    """Insert or update an HLTV player scoreboard record."""
    conn.execute(
        """
        INSERT INTO hltv_player_stats (
            match_id, event_id, map_name, team, player_id, player_nick,
            kills, deaths, plus_minus, adr, kast_pct, rating
        ) VALUES (
            :match_id, :event_id, :map_name, :team, :player_id, :player_nick,
            :kills, :deaths, :plus_minus, :adr, :kast_pct, :rating
        )
        ON CONFLICT(match_id, map_name, player_id) DO UPDATE SET
            team=excluded.team,
            player_nick=excluded.player_nick,
            kills=excluded.kills,
            deaths=excluded.deaths,
            plus_minus=excluded.plus_minus,
            adr=excluded.adr,
            kast_pct=excluded.kast_pct,
            rating=excluded.rating;
        """,
        stat,
    )


def upsert_hltv_map_stat(conn: sqlite3.Connection, map_stat: Dict[str, Any]) -> None:
    """Insert or update an HLTV map result record."""
    conn.execute(
        """
        INSERT INTO hltv_map_stats (
            match_id, event_id, map_name, team1, team2, score1, score2, half_scores
        ) VALUES (
            :match_id, :event_id, :map_name, :team1, :team2, :score1, :score2, :half_scores
        )
        ON CONFLICT(match_id, map_name) DO UPDATE SET
            team1=excluded.team1,
            team2=excluded.team2,
            score1=excluded.score1,
            score2=excluded.score2,
            half_scores=excluded.half_scores;
        """,
        map_stat,
    )
