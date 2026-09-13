#!/usr/bin/env python3
"""
Extract CS2 2D radar kill and duel spatial telemetry from parquet data lake.
Generates compact, client-optimized JSON datasets for top professional players
across all maps in the database into the portfolio public data directory.
"""

import glob
import json
import os
import sys
from collections import defaultdict
from typing import Dict, List, Optional, Tuple, Any

import polars as pl

# Official radar calibration parameters for all maps in the database
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
    "de_dust2": {"pos_x": -2476.0, "pos_y": 3239.0, "scale": 4.4},
    "dust2": {"pos_x": -2476.0, "pos_y": 3239.0, "scale": 4.4},
    "de_vertigo": {"pos_x": -3168.0, "pos_y": 1762.0, "scale": 4.0},
    "vertigo": {"pos_x": -3168.0, "pos_y": 1762.0, "scale": 4.0},
    "de_overpass": {"pos_x": -4820.0, "pos_y": 3540.0, "scale": 5.2},
    "overpass": {"pos_x": -4820.0, "pos_y": 3540.0, "scale": 5.2},
    "de_train": {"pos_x": -2308.0, "pos_y": 2078.0, "scale": 4.082077},
    "train": {"pos_x": -2308.0, "pos_y": 2078.0, "scale": 4.082077},
    "de_cache": {"pos_x": -2000.0, "pos_y": 3250.0, "scale": 5.5},
    "cache": {"pos_x": -2000.0, "pos_y": 3250.0, "scale": 5.5},
}

ALL_MAPS = [
    "mirage",
    "inferno",
    "nuke",
    "ancient",
    "anubis",
    "dust2",
    "vertigo",
    "overpass",
    "train",
    "cache",
]

PLAYER_PROFILES: Dict[str, Dict[str, str]] = {
    "donk": {"team": "Team Spirit", "role": "Rifler (Entry)", "country": "RU"},
    "m0NESY": {"team": "G2 Esports", "role": "AWPer (Sniper)", "country": "RU"},
    "ZywOo": {"team": "Team Vitality", "role": "AWPer / Hybrid", "country": "FR"},
    "NiKo": {"team": "G2 Esports", "role": "Rifler (Aggressive)", "country": "BA"},
    "b1t": {"team": "Natus Vincere", "role": "Rifler (Headshot Anchor)", "country": "UA"},
    "ropz": {"team": "FaZe Clan", "role": "Lurker / Anchor", "country": "EE"},
    "frozen": {"team": "FaZe Clan", "role": "Rifler (Support)", "country": "SK"},
    "flameZ": {"team": "Team Vitality", "role": "Rifler (Entry)", "country": "IL"},
    "Spinx": {"team": "Team Vitality", "role": "Lurker / Rifler", "country": "IL"},
    "torzsi": {"team": "MOUZ", "role": "AWPer (Sniper)", "country": "HU"},
    "broky": {"team": "FaZe Clan", "role": "AWPer (Sniper)", "country": "LV"},
    "XANTARES": {"team": "Eternal Fire", "role": "Rifler (Entry)", "country": "TR"},
    "apEX": {"team": "Team Vitality", "role": "IGL / Entry", "country": "FR"},
    "xertioN": {"team": "MOUZ", "role": "Rifler (Entry)", "country": "IL"},
    "w0nderful": {"team": "Natus Vincere", "role": "AWPer (Sniper)", "country": "UA"},
    "iM": {"team": "Natus Vincere", "role": "Rifler (Space Maker)", "country": "RO"},
    "KSCERATO": {"team": "FURIA", "role": "Rifler (Closer)", "country": "BR"},
    "bLitz": {"team": "The MongolZ", "role": "IGL / Rifler", "country": "MN"},
    "karrigan": {"team": "FaZe Clan", "role": "IGL / Entry", "country": "DK"},
    "FalleN": {"team": "FURIA", "role": "IGL / AWPer", "country": "BR"},
    "Twistzz": {"team": "Team Liquid", "role": "IGL / Rifler", "country": "CA"},
    "malbsMd": {"team": "G2 Esports", "role": "Rifler (Entry)", "country": "GT"},
    "Brollan": {"team": "MOUZ", "role": "Rifler (Aggressive)", "country": "SE"},
    "Jimpphat": {"team": "MOUZ", "role": "Anchor / Lurker", "country": "FI"},
    "TeSeS": {"team": "Heroic", "role": "Support / Anchor", "country": "DK"},
}

TARGET_PLAYERS = set(PLAYER_PROFILES.keys())


def normalize_coord(
    x: Optional[float], y: Optional[float], map_name: str
) -> Tuple[Optional[float], Optional[float]]:
    """Convert world coordinates into normalized [0.0, 1.0] radar coordinates."""
    if x is None or y is None:
        return None, None
    cal = MAP_RADAR_CALIBRATION.get(map_name.lower().replace("de_", ""))
    if not cal:
        return None, None
    rx = (float(x) - cal["pos_x"]) / (cal["scale"] * 1024.0)
    ry = (cal["pos_y"] - float(y)) / (cal["scale"] * 1024.0)
    if not (0.0 <= rx <= 1.0 and 0.0 <= ry <= 1.0):
        return None, None
    return round(rx, 4), round(ry, 4)


T_WEAPONS = {'glock', 'ak47', 'galilar', 'mac10', 'tec9', 'sg556', 'g3sg1', 'sawedoff'}
CT_WEAPONS = {'usp_silencer', 'hkp2000', 'm4a1_silencer', 'm4a1', 'famas', 'mp9', 'fiveseven', 'aug', 'scar20', 'mag7'}


def resolve_match_sides(df: pl.DataFrame) -> Dict[str, str]:
    """
    Determine the first-half starting side ('T' or 'CT') for all players in a match.
    Uses bipartite opponent graph 2-coloring weighted heavily by pistol rounds
    and side-exclusive weapon distribution.
    """
    opponents = defaultdict(set)
    first_half_votes = defaultdict(lambda: {'T': 0, 'CT': 0})

    for r in df.iter_rows(named=True):
        att = r.get('attacker_name')
        vic = r.get('victim_name')
        rnd = r.get('round_num')
        w = (r.get('weapon') or '').lower()
        if att and vic:
            opponents[att].add(vic)
            opponents[vic].add(att)
        if att and rnd:
            is_first_half = (rnd <= 12) or (25 <= rnd <= 27) or (31 <= rnd <= 33)
            weight = 10 if rnd in (1, 13) else 1
            if w in T_WEAPONS:
                first_half_votes[att]['T' if is_first_half else 'CT'] += weight
            elif w in CT_WEAPONS:
                first_half_votes[att]['CT' if is_first_half else 'T'] += weight

    all_players = set(opponents.keys())
    if not all_players:
        return {}

    team1 = set()
    team2 = set()
    visited = set()

    for start_p in sorted(all_players, key=lambda p: len(opponents[p]), reverse=True):
        if start_p in visited:
            continue
        queue = [(start_p, 1)]
        visited.add(start_p)
        team1.add(start_p)
        while queue:
            curr, team_num = queue.pop(0)
            next_team_num = 2 if team_num == 1 else 1
            for nxt in opponents[curr]:
                if nxt not in visited:
                    visited.add(nxt)
                    if next_team_num == 1:
                        team1.add(nxt)
                    else:
                        team2.add(nxt)
                    queue.append((nxt, next_team_num))

    team1_t_votes = sum(first_half_votes[p]['T'] for p in team1)
    team1_ct_votes = sum(first_half_votes[p]['CT'] for p in team1)
    team2_t_votes = sum(first_half_votes[p]['T'] for p in team2)
    team2_ct_votes = sum(first_half_votes[p]['CT'] for p in team2)

    team1_net_t = (team1_t_votes - team1_ct_votes) + (team2_ct_votes - team2_t_votes)

    player_first_half_side = {}
    if team1_net_t >= 0:
        for p in team1:
            player_first_half_side[p] = 'T'
        for p in team2:
            player_first_half_side[p] = 'CT'
    else:
        for p in team1:
            player_first_half_side[p] = 'CT'
        for p in team2:
            player_first_half_side[p] = 'T'

    return player_first_half_side


def extract_radar_data(
    lake_dir: str,
    output_dir: str,
    max_kills_per_player_map: int = 350,
    max_deaths_per_player_map: int = 200,
):
    print(f"Scanning parquet files in {lake_dir}...")
    files = glob.glob(os.path.join(lake_dir, "*/*/*/kills.parquet"))
    print(f"Found {len(files)} kill parquet files.")

    player_kills = defaultdict(lambda: defaultdict(list))
    player_deaths = defaultdict(lambda: defaultdict(list))

    total_files_processed = 0

    cols_needed = [
        "attacker_name", "victim_name", "map_name",
        "attacker_X", "attacker_Y", "victim_X", "victim_Y",
        "attacker_radar_x", "attacker_radar_y", "victim_radar_x", "victim_radar_y",
        "weapon", "headshot", "is_first_kill", "is_trade_kill", "distance", "round_num"
    ]

    for idx, f in enumerate(files):
        if idx > 0 and idx % 500 == 0:
            print(f"Processed {idx}/{len(files)} files...")
        try:
            df = pl.read_parquet(f)
            available_cols = [c for c in cols_needed if c in df.columns]
            sub_df = df.select(available_cols)

            match_sides = resolve_match_sides(sub_df)

            for row in sub_df.iter_rows(named=True):
                m_name = (row.get("map_name") or "").lower().replace("de_", "")
                if m_name not in ALL_MAPS:
                    continue

                att = row.get("attacker_name")
                vic = row.get("victim_name")

                att_is_target = att in TARGET_PLAYERS
                vic_is_target = vic in TARGET_PLAYERS

                if not (att_is_target or vic_is_target):
                    continue

                ax, ay = row.get("attacker_radar_x"), row.get("attacker_radar_y")
                vx, vy = row.get("victim_radar_x"), row.get("victim_radar_y")

                try:
                    ax = float(ax) if ax is not None else None
                    ay = float(ay) if ay is not None else None
                except (ValueError, TypeError):
                    ax, ay = None, None

                try:
                    vx = float(vx) if vx is not None else None
                    vy = float(vy) if vy is not None else None
                except (ValueError, TypeError):
                    vx, vy = None, None

                if ax is None or ay is None:
                    ax, ay = normalize_coord(row.get("attacker_X"), row.get("attacker_Y"), m_name)
                else:
                    ax, ay = round(ax, 4), round(ay, 4)

                if vx is None or vy is None:
                    vx, vy = normalize_coord(row.get("victim_X"), row.get("victim_Y"), m_name)
                else:
                    vx, vy = round(vx, 4), round(vy, 4)

                if ax is None or ay is None or vx is None or vy is None:
                    continue

                if not (0.0 <= ax <= 1.0 and 0.0 <= ay <= 1.0 and 0.0 <= vx <= 1.0 and 0.0 <= vy <= 1.0):
                    continue

                raw_dist = row.get("distance")
                dist_m = round(float(raw_dist), 1) if raw_dist is not None else None
                weapon = (row.get("weapon") or "unknown").lower()
                headshot = bool(row.get("headshot", False))
                first_kill = bool(row.get("is_first_kill", False))
                trade_kill = bool(row.get("is_trade_kill", False))
                round_num = int(row.get("round_num") or 1)

                is_first_half = (round_num <= 12) or (25 <= round_num <= 27) or (31 <= round_num <= 33)
                att_side = None
                if att and att in match_sides:
                    att_side = match_sides[att] if is_first_half else ('CT' if match_sides[att] == 'T' else 'T')
                elif vic and vic in match_sides:
                    vic_side_start = match_sides[vic]
                    vic_curr_side = vic_side_start if is_first_half else ('CT' if vic_side_start == 'T' else 'T')
                    att_side = 'CT' if vic_curr_side == 'T' else 'T'
                else:
                    if weapon in T_WEAPONS:
                        att_side = 'T'
                    elif weapon in CT_WEAPONS:
                        att_side = 'CT'
                    else:
                        att_side = 'T'

                vic_side = 'CT' if att_side == 'T' else 'T'

                event_data = {
                    "m": m_name,
                    "ax": ax,
                    "ay": ay,
                    "vx": vx,
                    "vy": vy,
                    "w": weapon,
                    "hs": headshot,
                    "fk": first_kill,
                    "tk": trade_kill,
                    "d": dist_m,
                    "rnd": round_num,
                }

                if att_is_target:
                    if len(player_kills[att][m_name]) < max_kills_per_player_map:
                        e = dict(event_data)
                        e["s"] = att_side
                        e["opp"] = vic or "Opponent"
                        player_kills[att][m_name].append(e)

                if vic_is_target:
                    if len(player_deaths[vic][m_name]) < max_deaths_per_player_map:
                        e = dict(event_data)
                        e["s"] = vic_side
                        e["opp"] = att or "Opponent"
                        player_deaths[vic][m_name].append(e)

            total_files_processed += 1
        except Exception:
            continue

    print(f"Extraction complete. Total files processed: {total_files_processed}")
    os.makedirs(output_dir, exist_ok=True)

    manifest_players = []

    for player in sorted(TARGET_PLAYERS):
        profile = PLAYER_PROFILES.get(player, {})
        all_kills = []
        all_deaths = []
        map_stats = {}

        total_hs = 0
        total_dist_sum = 0.0
        total_dist_count = 0
        opening_duels_won = 0
        opening_duels_total = 0

        for m_name in ALL_MAPS:
            k_list = player_kills[player].get(m_name, [])
            d_list = player_deaths[player].get(m_name, [])
            all_kills.extend(k_list)
            all_deaths.extend(d_list)

            map_stats[m_name] = {
                "kills": len(k_list),
                "deaths": len(d_list),
            }

            for k in k_list:
                if k["hs"]:
                    total_hs += 1
                if k["d"] is not None and k["d"] > 0:
                    total_dist_sum += k["d"]
                    total_dist_count += 1
                if k["fk"]:
                    opening_duels_won += 1
                    opening_duels_total += 1

            for d in d_list:
                if d["fk"]:
                    opening_duels_total += 1

        total_k = len(all_kills)
        total_d = len(all_deaths)
        hs_pct = round((total_hs / total_k * 100.0), 1) if total_k > 0 else 0.0
        avg_dist = round((total_dist_sum / total_dist_count), 1) if total_dist_count > 0 else 0.0
        fk_win_pct = round((opening_duels_won / opening_duels_total * 100.0), 1) if opening_duels_total > 0 else 0.0

        player_data = {
            "player": player,
            "team": profile.get("team", ""),
            "role": profile.get("role", ""),
            "country": profile.get("country", ""),
            "stats": {
                "totalKills": total_k,
                "totalDeaths": total_d,
                "headshotPct": hs_pct,
                "avgDistanceMeters": avg_dist,
                "openingDuelWinPct": fk_win_pct,
                "maps": map_stats,
            },
            "kills": all_kills,
            "deaths": all_deaths,
        }

        player_filepath = os.path.join(output_dir, f"{player}.json")
        with open(player_filepath, "w", encoding="utf-8") as out_f:
            json.dump(player_data, out_f, separators=(",", ":"))

        file_size_kb = round(os.path.getsize(player_filepath) / 1024, 1)
        print(f"Wrote {player}.json: {total_k} kills, {total_d} deaths ({file_size_kb} KB)")

        manifest_players.append({
            "player": player,
            "team": profile.get("team", ""),
            "role": profile.get("role", ""),
            "country": profile.get("country", ""),
            "totalKills": total_k,
            "totalDeaths": total_d,
            "headshotPct": hs_pct,
            "avgDistanceMeters": avg_dist,
            "openingDuelWinPct": fk_win_pct,
            "maps": [m for m in ALL_MAPS if map_stats[m]["kills"] > 0 or map_stats[m]["deaths"] > 0],
        })

    manifest = {
        "version": "1.0",
        "generatedAt": "2026-09-13",
        "allMaps": ALL_MAPS,
        "players": manifest_players,
        "weaponCategories": {
            "rifles": ["ak47", "m4a1_silencer", "m4a1", "galilar", "famas", "sg556", "aug"],
            "snipers": ["awp", "ssg08", "g3sg1", "scar20"],
            "pistols": ["deagle", "usp_silencer", "glock", "p250", "cz75a", "tec9", "fiveseven", "elite", "revolver"],
            "smg_heavy": ["mp9", "mac10", "mp7", "mp5sd", "ump45", "p90", "bizon", "nova", "xm1014", "mag7", "sawedoff", "m249", "negev"]
        }
    }

    manifest_filepath = os.path.join(output_dir, "manifest.json")
    with open(manifest_filepath, "w", encoding="utf-8") as out_f:
        json.dump(manifest, out_f, indent=2)

    print(f"Manifest successfully written to {manifest_filepath}")


if __name__ == "__main__":
    lake_directory = os.path.abspath("data/lake")
    output_directory = os.path.abspath("../portfolio/public/data/cs2/radar")
    if len(sys.argv) > 1:
        lake_directory = sys.argv[1]
    if len(sys.argv) > 2:
        output_directory = sys.argv[2]
    extract_radar_data(lake_directory, output_directory)
