"""Interactive data explorer for the CS2 Parquet lake and HLTV database."""
import argparse
import sqlite3
from pathlib import Path

import polars as pl

from src.config import DATABASE_PATH, LAKE_DIR


def show_kills_leaderboard(lake_path: Path = LAKE_DIR, limit: int = 10) -> None:
    """Display top elimination leaders across the entire tournament lake."""
    files = list(lake_path.glob("*/*/*/kills.parquet"))
    if not files:
        print("No kills.parquet files found in data lake.")
        return

    kills = pl.read_parquet(files, columns=["attacker_name", "headshot", "distance"])
    df = (
        kills.group_by("attacker_name")
        .agg([
            pl.len().alias("kills"),
            pl.col("headshot").sum().alias("headshots"),
            ((pl.col("headshot").sum() / pl.len()) * 100).round(1).alias("hs_pct"),
            pl.col("distance").mean().round(1).alias("avg_distance"),
        ])
        .sort("kills", descending=True)
        .head(limit)
    )
    print(f"\n=== Top {limit} Kill Leaders (Total Kills: {len(kills):,}) ===")
    print(df)


def show_opening_duels(lake_path: Path = LAKE_DIR, limit: int = 10) -> None:
    """Display top opening duel winners and first-blood leaders."""
    files = list(lake_path.glob("*/*/*/kills.parquet"))
    if not files:
        print("No kills.parquet files found.")
        return

    kills = pl.read_parquet(files, columns=["is_first_kill", "attacker_name", "weapon", "headshot"])
    df = (
        kills.filter(pl.col("is_first_kill") == True)
        .group_by("attacker_name")
        .agg([
            pl.len().alias("opening_kills"),
            pl.col("weapon").mode().first().alias("top_weapon"),
            pl.col("headshot").sum().alias("headshots"),
        ])
        .sort("opening_kills", descending=True)
        .head(limit)
    )
    print(f"\n=== Top {limit} Opening Duel Leaders ===")
    print(df)


def show_fastest_traders(lake_path: Path = LAKE_DIR, min_trades: int = 15, limit: int = 10) -> None:
    """Display players with the fastest trade kill reaction latency."""
    files = list(lake_path.glob("*/*/*/kills.parquet"))
    if not files:
        print("No kills.parquet files found.")
        return

    kills = pl.read_parquet(files, columns=["is_trade_kill", "attacker_name", "trade_ticks_delta"])
    df = (
        kills.filter(pl.col("is_trade_kill") == True)
        .group_by("attacker_name")
        .agg([
            pl.len().alias("trade_kills"),
            (pl.col("trade_ticks_delta").mean() / 64.0).round(2).alias("avg_latency_sec"),
            pl.col("trade_ticks_delta").mean().round(1).alias("avg_ticks"),
        ])
        .filter(pl.col("trade_kills") >= min_trades)
        .sort("avg_latency_sec")
        .head(limit)
    )
    print(f"\n=== Fastest Trade Reaction Latency (min {min_trades} trades) ===")
    print(df)


def show_hltv_round_swings(db_path: Path = DATABASE_PATH, limit: int = 10) -> None:
    """Display top HLTV Round Swing (Rating 3.0) map performances from SQLite."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    query = """
        SELECT match_id, map_name, team, player_nick, kills, deaths,
               rating, round_swing, adr, kast_pct
        FROM hltv_player_stats
        WHERE round_swing IS NOT NULL
        ORDER BY round_swing DESC
        LIMIT ?
    """
    rows = conn.execute(query, (limit,)).fetchall()
    print(f"\n=== Top {limit} HLTV Round Swing Map Performances (Rating 3.0) ===")
    print(f"{'Player':<16} {'Team':<16} {'Map':<10} {'K-D':<8} {'Rating':<8} {'Swing %':<8} {'ADR':<6}")
    print("-" * 76)
    for r in rows:
        kd = f"{r['kills']}-{r['deaths']}"
        swing = f"{r['round_swing']:+.2f}%"
        print(f"{r['player_nick']:<16} {r['team']:<16} {r['map_name']:<10} {kd:<8} {r['rating']:<8} {swing:<8} {r['adr']:<6}")
    conn.close()


def show_utility_breakdown(lake_path: Path = LAKE_DIR) -> None:
    """Display summary of grenades detonated by type and map."""
    files = list(lake_path.glob("*/*/*/utility.parquet"))
    if not files:
        print("No utility.parquet files found.")
        return

    util = pl.read_parquet(files, columns=["grenade_type", "map_name"])
    by_type = util.group_by("grenade_type").len().sort("len", descending=True)
    by_map = util.group_by("map_name").len().sort("len", descending=True)
    print(f"\n=== Utility Detonation Totals ({len(util):,} total) ===")
    print(by_type)
    print("\n=== Utility Detonations by Map ===")
    print(by_map)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="CS2 Analytics & Parquet Lake Explorer.")
    parser.add_argument("--kills", action="store_true", help="Display top fraggers leaderboard")
    parser.add_argument("--openers", action="store_true", help="Display opening duel winners")
    parser.add_argument("--trades", action="store_true", help="Display fastest trade reaction latency")
    parser.add_argument("--utility", action="store_true", help="Display utility breakdown")
    parser.add_argument("--swing", action="store_true", help="Display top HLTV round swing ratings")
    parser.add_argument("--all", action="store_true", help="Display all summaries")
    args = parser.parse_args()

    if args.all or not any([args.kills, args.openers, args.trades, args.utility, args.swing]):
        show_kills_leaderboard()
        show_opening_duels()
        show_fastest_traders()
        show_hltv_round_swings()
        show_utility_breakdown()
    else:
        if args.kills:
            show_kills_leaderboard()
        if args.openers:
            show_opening_duels()
        if args.trades:
            show_fastest_traders()
        if args.utility:
            show_utility_breakdown()
        if args.swing:
            show_hltv_round_swings()
