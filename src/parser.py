"""CS2 demo analytics and feature extraction engine.

Extracts high-resolution combat telemetry, trade kills, opening duels,
utility detonations, and round state from CS2 Source 2 (.dem) files,
exporting normalized columnar Parquet tables to the data lake.
"""
import logging
import re
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Tuple

import pandas as pd
import polars as pl
from demoparser2 import DemoParser

from src.config import LAKE_DIR

logger = logging.getLogger(__name__)

# Official radar calibration parameters for standard competitive CS2 maps
# Formula:
#   radar_x = (world_x - pos_x) / (scale * 1024.0)
#   radar_y = (pos_y - world_y) / (scale * 1024.0)
MAP_RADAR_CALIBRATION: Dict[str, Dict[str, float]] = {
    "de_anubis": {"pos_x": -2796.0, "pos_y": 3328.0, "scale": 5.22},
    "anubis": {"pos_x": -2796.0, "pos_y": 3328.0, "scale": 5.22},
    "de_ancient": {"pos_x": -2953.0, "pos_y": 2164.0, "scale": 5.0},
    "ancient": {"pos_x": -2953.0, "pos_y": 2164.0, "scale": 5.0},
    "de_inferno": {"pos_x": -2087.0, "pos_y": 3870.0, "scale": 4.9},
    "inferno": {"pos_x": -2087.0, "pos_y": 3870.0, "scale": 4.9},
    "de_mirage": {"pos_x": -3230.0, "pos_y": 1713.0, "scale": 5.0},
    "mirage": {"pos_x": -3230.0, "pos_y": 1713.0, "scale": 5.0},
    "de_nuke": {"pos_x": -3453.0, "pos_y": 2887.0, "scale": 7.0},
    "nuke": {"pos_x": -3453.0, "pos_y": 2887.0, "scale": 7.0},
    "de_vertigo": {"pos_x": -3168.0, "pos_y": 1762.0, "scale": 4.0},
    "vertigo": {"pos_x": -3168.0, "pos_y": 1762.0, "scale": 4.0},
    "de_dust2": {"pos_x": -2476.0, "pos_y": 3239.0, "scale": 4.4},
    "dust2": {"pos_x": -2476.0, "pos_y": 3239.0, "scale": 4.4},
    "de_overpass": {"pos_x": -4820.0, "pos_y": 3540.0, "scale": 5.2},
    "overpass": {"pos_x": -4820.0, "pos_y": 3540.0, "scale": 5.2},
}

ROUND_WIN_REASONS: Dict[int, str] = {
    1: "target_bombed",
    7: "bomb_defused",
    8: "cts_win",
    9: "terrorists_win",
    10: "round_draw",
    12: "target_saved",
}

# 64-tick server standard: 3.0 seconds = 192 ticks
DEFAULT_TRADE_WINDOW_TICKS = 192


def to_df(data: Any) -> pd.DataFrame:
    """Safely convert demoparser2 event result (DataFrame or list of dicts) to DataFrame."""
    if isinstance(data, pd.DataFrame):
        return data
    if isinstance(data, list) and len(data) > 0:
        return pd.DataFrame(data)
    return pd.DataFrame()


def normalize_radar_coords(
    x: Optional[float], y: Optional[float], map_name: str
) -> Tuple[Optional[float], Optional[float]]:
    """Convert in-game 3D world coordinates into normalized 2D radar coordinates [0.0, 1.0]."""
    if x is None or y is None or pd.isna(x) or pd.isna(y):
        return None, None

    clean_map = map_name.lower().replace(" ", "_")
    cal = MAP_RADAR_CALIBRATION.get(clean_map)
    if not cal:
        if clean_map.startswith("de_"):
            cal = MAP_RADAR_CALIBRATION.get(clean_map[3:])
        else:
            cal = MAP_RADAR_CALIBRATION.get(f"de_{clean_map}")

    if not cal:
        return None, None

    radar_x = (float(x) - cal["pos_x"]) / (cal["scale"] * 1024.0)
    radar_y = (cal["pos_y"] - float(y)) / (cal["scale"] * 1024.0)
    return round(radar_x, 5), round(radar_y, 5)


def build_round_timeline(
    parser: DemoParser,
) -> Tuple[pd.DataFrame, Callable[[int], int], int]:
    """Identify live rounds, freeze periods, end ticks, and winners."""
    ms = to_df(parser.parse_event("round_announce_match_start"))
    match_start_tick = int(ms.iloc[0]["tick"]) if not ms.empty else 0

    re = to_df(parser.parse_event("round_end"))
    if not re.empty and match_start_tick > 0:
        re = re[re["tick"] > match_start_tick].reset_index(drop=True)

    fe = to_df(parser.parse_event("round_freeze_end"))
    if not fe.empty and match_start_tick > 0:
        fe = fe[fe["tick"] >= match_start_tick].reset_index(drop=True)

    rounds = []
    end_ticks = []
    total_rounds = len(re)

    for i in range(total_rounds):
        round_end_tick = int(re.iloc[i]["tick"])
        freeze_end_tick = int(fe.iloc[i]["tick"]) if i < len(fe) else None
        raw_winner = re.iloc[i]["winner"] if "winner" in re.columns else None
        if raw_winner in (2, "2", "T", "t", "TERRORIST"):
            winner_str = "T"
            winner_code = 2
        elif raw_winner in (3, "3", "CT", "ct", "COUNTER_TERRORIST"):
            winner_str = "CT"
            winner_code = 3
        else:
            winner_str = "UNKNOWN"
            winner_code = None

        raw_reason = re.iloc[i]["reason"] if "reason" in re.columns else None
        try:
            reason_code = int(raw_reason) if pd.notna(raw_reason) else None
        except (ValueError, TypeError):
            reason_code = None
        reason_str = ROUND_WIN_REASONS.get(reason_code, f"reason_{reason_code}")

        duration_ticks = (
            round_end_tick - freeze_end_tick if freeze_end_tick is not None else None
        )
        duration_seconds = (
            round(duration_ticks / 64.0, 2) if duration_ticks is not None else None
        )

        rounds.append(
            {
                "round_num": i + 1,
                "freeze_end_tick": freeze_end_tick,
                "end_tick": round_end_tick,
                "duration_ticks": duration_ticks,
                "duration_seconds": duration_seconds,
                "winner": winner_str,
                "reason_code": reason_code,
                "reason_name": reason_str,
            }
        )
        end_ticks.append(round_end_tick)

    df_rounds = pd.DataFrame(rounds)

    def round_mapper(tick: int) -> int:
        for idx, end_tick in enumerate(end_ticks):
            if tick <= end_tick:
                return idx + 1
        return max(1, len(end_ticks))

    return df_rounds, round_mapper, match_start_tick


def extract_kills(
    parser: DemoParser,
    round_mapper: Callable[[int], int],
    match_start_tick: int,
    map_name: str,
    trade_window_ticks: int = DEFAULT_TRADE_WINDOW_TICKS,
) -> pd.DataFrame:
    """Extract player kills with 3D coordinates, radar normalization, and trade logic."""
    kills = to_df(
        parser.parse_event(
            "player_death", player=["X", "Y", "Z", "pitch", "yaw", "last_place_name"]
        )
    )
    if kills.empty:
        return pd.DataFrame()

    if match_start_tick > 0:
        kills = kills[kills["tick"] >= match_start_tick].copy()

    kills["round_num"] = kills["tick"].apply(round_mapper)
    kills = kills.sort_values("tick").reset_index(drop=True)

    # Radar coordinates
    norm_attacker = [
        normalize_radar_coords(x, y, map_name)
        for x, y in zip(kills.get("attacker_X", []), kills.get("attacker_Y", []))
    ]
    norm_victim = [
        normalize_radar_coords(x, y, map_name)
        for x, y in zip(kills.get("user_X", []), kills.get("user_Y", []))
    ]

    kills["attacker_radar_x"] = [p[0] for p in norm_attacker]
    kills["attacker_radar_y"] = [p[1] for p in norm_attacker]
    kills["victim_radar_x"] = [p[0] for p in norm_victim]
    kills["victim_radar_y"] = [p[1] for p in norm_victim]

    # Initialize trade kill attributes
    kills["is_first_kill"] = False
    kills["is_trade_kill"] = False
    kills["traded_player_name"] = None
    kills["traded_player_steamid"] = None
    kills["trade_ticks_delta"] = None
    kills["was_traded"] = False

    # Attribute trade kills per round
    for _, group in kills.groupby("round_num"):
        indices = group.index.tolist()
        if not indices:
            continue
        # First kill in round
        kills.loc[indices[0], "is_first_kill"] = True

        for i in range(len(indices)):
            idx_curr = indices[i]
            curr_tick = kills.loc[idx_curr, "tick"]
            curr_attacker_id = kills.loc[idx_curr, "attacker_steamid"]
            curr_victim_id = kills.loc[idx_curr, "user_steamid"]

            for j in range(i - 1, -1, -1):
                idx_prior = indices[j]
                prior_tick = kills.loc[idx_prior, "tick"]
                if curr_tick - prior_tick > trade_window_ticks:
                    break

                prior_attacker_id = kills.loc[idx_prior, "attacker_steamid"]
                prior_victim_id = kills.loc[idx_prior, "user_steamid"]

                # A trade occurs when the current victim was the prior attacker,
                # avenged by another player
                if (
                    curr_victim_id == prior_attacker_id
                    and curr_attacker_id != prior_victim_id
                ):
                    kills.loc[idx_curr, "is_trade_kill"] = True
                    kills.loc[idx_curr, "traded_player_name"] = kills.loc[
                        idx_prior, "user_name"
                    ]
                    kills.loc[idx_curr, "traded_player_steamid"] = kills.loc[
                        idx_prior, "user_steamid"
                    ]
                    kills.loc[idx_curr, "trade_ticks_delta"] = int(
                        curr_tick - prior_tick
                    )
                    kills.loc[idx_prior, "was_traded"] = True
                    break

    kills["trade_ticks_delta"] = pd.to_numeric(
        kills["trade_ticks_delta"], errors="coerce"
    ).astype("Int64")

    # Standardize column naming
    rename_cols = {
        "user_name": "victim_name",
        "user_steamid": "victim_steamid",
        "user_X": "victim_X",
        "user_Y": "victim_Y",
        "user_Z": "victim_Z",
        "user_pitch": "victim_pitch",
        "user_yaw": "victim_yaw",
        "user_last_place_name": "victim_last_place_name",
    }
    kills = kills.rename(columns=rename_cols)

    desired_cols = [
        "round_num",
        "tick",
        "attacker_name",
        "attacker_steamid",
        "attacker_X",
        "attacker_Y",
        "attacker_Z",
        "attacker_radar_x",
        "attacker_radar_y",
        "attacker_pitch",
        "attacker_yaw",
        "attacker_last_place_name",
        "victim_name",
        "victim_steamid",
        "victim_X",
        "victim_Y",
        "victim_Z",
        "victim_radar_x",
        "victim_radar_y",
        "victim_pitch",
        "victim_yaw",
        "victim_last_place_name",
        "assister_name",
        "assister_steamid",
        "weapon",
        "headshot",
        "hitgroup",
        "distance",
        "penetrated",
        "thrusmoke",
        "attackerblind",
        "noscope",
        "assistedflash",
        "is_first_kill",
        "is_trade_kill",
        "traded_player_name",
        "traded_player_steamid",
        "trade_ticks_delta",
        "was_traded",
    ]
    cols_to_keep = [c for c in desired_cols if c in kills.columns]
    return kills[cols_to_keep]


def extract_damage(
    parser: DemoParser,
    round_mapper: Callable[[int], int],
    match_start_tick: int,
    map_name: str,
) -> pd.DataFrame:
    """Extract individual damage events with coordinates."""
    dmg = to_df(
        parser.parse_event(
            "player_hurt", player=["X", "Y", "Z", "pitch", "yaw"]
        )
    )
    if dmg.empty:
        return pd.DataFrame()

    if match_start_tick > 0:
        dmg = dmg[dmg["tick"] >= match_start_tick].copy()

    dmg["round_num"] = dmg["tick"].apply(round_mapper)

    norm_attacker = [
        normalize_radar_coords(x, y, map_name)
        for x, y in zip(dmg.get("attacker_X", []), dmg.get("attacker_Y", []))
    ]
    norm_victim = [
        normalize_radar_coords(x, y, map_name)
        for x, y in zip(dmg.get("user_X", []), dmg.get("user_Y", []))
    ]

    dmg["attacker_radar_x"] = [p[0] for p in norm_attacker]
    dmg["attacker_radar_y"] = [p[1] for p in norm_attacker]
    dmg["victim_radar_x"] = [p[0] for p in norm_victim]
    dmg["victim_radar_y"] = [p[1] for p in norm_victim]

    rename_cols = {
        "user_name": "victim_name",
        "user_steamid": "victim_steamid",
        "user_X": "victim_X",
        "user_Y": "victim_Y",
        "user_Z": "victim_Z",
        "user_pitch": "victim_pitch",
        "user_yaw": "victim_yaw",
    }
    dmg = dmg.rename(columns=rename_cols)

    desired_cols = [
        "round_num",
        "tick",
        "attacker_name",
        "attacker_steamid",
        "attacker_X",
        "attacker_Y",
        "attacker_Z",
        "attacker_radar_x",
        "attacker_radar_y",
        "victim_name",
        "victim_steamid",
        "victim_X",
        "victim_Y",
        "victim_Z",
        "victim_radar_x",
        "victim_radar_y",
        "weapon",
        "hitgroup",
        "dmg_health",
        "dmg_armor",
        "health",
        "armor",
    ]
    cols_to_keep = [c for c in desired_cols if c in dmg.columns]
    return dmg[cols_to_keep]


def extract_utility(
    parser: DemoParser,
    round_mapper: Callable[[int], int],
    match_start_tick: int,
    map_name: str,
) -> pd.DataFrame:
    """Extract utility detonation events (smokes, flashbangs, HEs, molotovs)."""
    utility_event_types = {
        "smokegrenade_detonate": "smoke",
        "flashbang_detonate": "flash",
        "hegrenade_detonate": "he",
        "inferno_startburn": "molotov",
    }

    frames = []
    for evt_name, grenade_label in utility_event_types.items():
        try:
            df = to_df(parser.parse_event(evt_name))
            if not df.empty:
                df["grenade_type"] = grenade_label
                frames.append(df)
        except Exception:
            continue

    if not frames:
        return pd.DataFrame()

    all_util = pd.concat(frames, ignore_index=True)
    if match_start_tick > 0:
        all_util = all_util[all_util["tick"] >= match_start_tick].copy()

    all_util["round_num"] = all_util["tick"].apply(round_mapper)

    norm_coords = [
        normalize_radar_coords(x, y, map_name)
        for x, y in zip(all_util.get("x", []), all_util.get("y", []))
    ]
    all_util["radar_x"] = [p[0] for p in norm_coords]
    all_util["radar_y"] = [p[1] for p in norm_coords]

    rename_cols = {
        "user_name": "thrower_name",
        "user_steamid": "thrower_steamid",
    }
    all_util = all_util.rename(columns=rename_cols)

    desired_cols = [
        "round_num",
        "tick",
        "grenade_type",
        "thrower_name",
        "thrower_steamid",
        "x",
        "y",
        "z",
        "radar_x",
        "radar_y",
        "entityid",
    ]
    cols_to_keep = [c for c in desired_cols if c in all_util.columns]
    return all_util[cols_to_keep].sort_values("tick").reset_index(drop=True)


def extract_bomb_events(
    parser: DemoParser,
    round_mapper: Callable[[int], int],
    match_start_tick: int,
    map_name: str,
) -> pd.DataFrame:
    """Extract bomb plant, defusal, and explosion events."""
    frames = []

    try:
        bp = to_df(parser.parse_event("bomb_planted", player=["X", "Y", "Z"]))
        if not bp.empty:
            bp["event_type"] = "planted"
            frames.append(bp)
    except Exception:
        pass

    try:
        bd = to_df(parser.parse_event("bomb_defused", player=["X", "Y", "Z"]))
        if not bd.empty:
            bd["event_type"] = "defused"
            frames.append(bd)
    except Exception:
        pass

    try:
        bx = to_df(parser.parse_event("bomb_exploded"))
        if not bx.empty:
            bx["event_type"] = "exploded"
            frames.append(bx)
    except Exception:
        pass

    if not frames:
        return pd.DataFrame()

    all_bomb = pd.concat(frames, ignore_index=True)
    if match_start_tick > 0:
        all_bomb = all_bomb[all_bomb["tick"] >= match_start_tick].copy()

    all_bomb["round_num"] = all_bomb["tick"].apply(round_mapper)

    # Resolve coordinates
    x_col = "user_X" if "user_X" in all_bomb.columns else ("x" if "x" in all_bomb.columns else None)
    y_col = "user_Y" if "user_Y" in all_bomb.columns else ("y" if "y" in all_bomb.columns else None)
    z_col = "user_Z" if "user_Z" in all_bomb.columns else ("z" if "z" in all_bomb.columns else None)

    xs = all_bomb[x_col] if x_col else [None] * len(all_bomb)
    ys = all_bomb[y_col] if y_col else [None] * len(all_bomb)
    all_bomb["z"] = all_bomb[z_col] if z_col else [None] * len(all_bomb)
    all_bomb["x"] = xs
    all_bomb["y"] = ys

    norm_coords = [normalize_radar_coords(x, y, map_name) for x, y in zip(xs, ys)]
    all_bomb["radar_x"] = [p[0] for p in norm_coords]
    all_bomb["radar_y"] = [p[1] for p in norm_coords]

    rename_cols = {
        "user_name": "player_name",
        "user_steamid": "player_steamid",
    }
    all_bomb = all_bomb.rename(columns=rename_cols)

    desired_cols = [
        "round_num",
        "tick",
        "event_type",
        "player_name",
        "player_steamid",
        "site",
        "x",
        "y",
        "z",
        "radar_x",
        "radar_y",
    ]
    cols_to_keep = [c for c in desired_cols if c in all_bomb.columns]
    return all_bomb[cols_to_keep].sort_values("tick").reset_index(drop=True)


def parse_demo_to_lake(
    demo_path: Path,
    lake_root: Path = LAKE_DIR,
    match_id: Optional[int] = None,
    event_id: Optional[int] = None,
) -> Dict[str, Any]:
    """Parse a CS2 demo and write Snappy-compressed Parquet tables into the data lake."""
    demo_path = Path(demo_path)
    if not demo_path.exists():
        raise FileNotFoundError(f"Demo file not found: {demo_path}")

    logger.info("Initializing parser for %s", demo_path.name)
    parser = DemoParser(str(demo_path))

    # Read map from demo header
    header = parser.parse_header()
    raw_map = header.get("map_name", "unknown_map")
    clean_map = raw_map.replace("de_", "")

    # Multi-part map handling (e.g. mirage-p1 vs mirage-p2)
    part_match = re.search(r"[-_](p\d+)", demo_path.stem, re.I)
    map_folder = f"{clean_map}_{part_match.group(1).lower()}" if part_match else clean_map

    # Output directory partitioning: lake/event_{id}/match_{id}/{map_folder}/
    ev_str = f"event_{event_id}" if event_id else "event_unknown"
    ma_str = f"match_{match_id}" if match_id else "match_unknown"
    target_dir = lake_root / ev_str / ma_str / map_folder
    target_dir.mkdir(parents=True, exist_ok=True)

    # 1. Round timeline
    df_rounds, round_mapper, match_start_tick = build_round_timeline(parser)

    # 2. Combat events
    df_kills = extract_kills(parser, round_mapper, match_start_tick, raw_map)
    df_damage = extract_damage(parser, round_mapper, match_start_tick, raw_map)
    df_utility = extract_utility(parser, round_mapper, match_start_tick, raw_map)
    df_bomb = extract_bomb_events(parser, round_mapper, match_start_tick, raw_map)

    # Attach match context identifiers
    for df in (df_rounds, df_kills, df_damage, df_utility, df_bomb):
        if not df.empty:
            df.insert(0, "map_name", clean_map)
            df.insert(0, "match_id", match_id)
            df.insert(0, "event_id", event_id)

    # Write tables to Parquet (Snappy compressed)
    tables = {
        "rounds.parquet": df_rounds,
        "kills.parquet": df_kills,
        "damage.parquet": df_damage,
        "utility.parquet": df_utility,
        "bomb.parquet": df_bomb,
    }

    results: Dict[str, Any] = {
        "demo_file": demo_path.name,
        "map": clean_map,
        "raw_map": raw_map,
        "target_directory": str(target_dir),
        "total_rounds": len(df_rounds),
        "total_kills": len(df_kills),
        "trade_kills": int(df_kills["is_trade_kill"].sum()) if not df_kills.empty else 0,
        "opening_kills": int(df_kills["is_first_kill"].sum()) if not df_kills.empty else 0,
        "total_damage_events": len(df_damage),
        "total_utility_events": len(df_utility),
        "total_bomb_events": len(df_bomb),
        "files_written": {},
    }

    total_bytes = 0
    for file_name, df in tables.items():
        out_file = target_dir / file_name
        if not df.empty:
            pl_df = pl.from_pandas(df)
            pl_df.write_parquet(out_file, compression="snappy")
            size_kb = round(out_file.stat().st_size / 1024, 2)
            total_bytes += out_file.stat().st_size
            results["files_written"][file_name] = f"{size_kb} KB"
        else:
            results["files_written"][file_name] = "0 KB (empty)"

    results["total_lake_size_mb"] = round(total_bytes / (1024 * 1024), 3)
    logger.info(
        "Successfully parsed %s -> %d rounds, %d kills in %s MB",
        clean_map,
        results["total_rounds"],
        results["total_kills"],
        results["total_lake_size_mb"],
    )
    return results
