"""Extractor for official HLTV match stats, player scoreboards, and map results."""
import logging
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional
from bs4 import BeautifulSoup

from src.config import MATCHES_CACHE_DIR
from src.db import get_db, upsert_hltv_map_stat, upsert_hltv_player_stat

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cs2_pipeline.harvest_stats")


def parse_hltv_match_stats(html_content: str, match_id: int, event_id: int) -> Dict[str, Any]:
    """Extract official HLTV map results and player scoreboards from a match page HTML."""
    soup = BeautifulSoup(html_content, "html.parser")

    # 1. Parse map tabs to map container IDs to human-readable map names
    map_tabs: Dict[str, str] = {}
    for link in soup.find_all("div", class_="stats-menu-link"):
        short_elem = link.find(class_=lambda c: c and "short" in c)
        full_elem = link.find(class_=lambda c: c and "full" in c)
        s_id = short_elem.get("id") if short_elem else None
        f_name = full_elem.text.strip() if full_elem else None
        if s_id and f_name:
            map_tabs[s_id] = f_name

    # 2. Parse map results and half-time splits from mapholder divs
    map_results: List[Dict[str, Any]] = []
    for mh in soup.find_all("div", class_="mapholder"):
        name_elem = mh.find("div", class_="mapname")
        results_div = mh.find("div", class_="results")
        if not name_elem or not results_div:
            continue

        map_name = name_elem.text.strip()
        teams = results_div.find_all(class_=lambda c: c and "results-teamname" in c)
        scores = results_div.find_all(class_=lambda c: c and "results-team-score" in c)

        team1 = teams[0].text.strip() if len(teams) > 0 else "Team 1"
        team2 = teams[1].text.strip() if len(teams) > 1 else "Team 2"
        score1 = int(scores[0].text.strip()) if len(scores) > 0 and scores[0].text.strip().isdigit() else 0
        score2 = int(scores[1].text.strip()) if len(scores) > 1 and scores[1].text.strip().isdigit() else 0

        # Half scores: e.g. (5:7; 8:4)
        half_scores_elem = results_div.find(class_=lambda c: c and ("halfscore" in c or "decay" in c))
        half_scores = half_scores_elem.text.strip() if half_scores_elem else None

        map_results.append({
            "match_id": match_id,
            "event_id": event_id,
            "map_name": map_name,
            "team1": team1,
            "team2": team2,
            "score1": score1,
            "score2": score2,
            "half_scores": half_scores,
        })

    # 3. Parse player boxscore tables (skip 'All maps' / series aggregate sections)
    player_stats: List[Dict[str, Any]] = []
    for sc in soup.find_all("div", class_="stats-content"):
        sc_id = sc.get("id", "").replace("-content", "")
        map_name = map_tabs.get(sc_id, sc_id if sc_id else "All maps")

        # Filter out 'All maps' / overall series summary
        if sc_id.lower() == "all" or map_name.strip().lower() in ("all", "all maps", "overview"):
            continue

        for table in sc.find_all("table", class_="totalstats"):
            first_tr = table.find("tr")
            if not first_tr:
                continue

            th_cells = [x.text.strip() for x in first_tr.find_all(["th", "td"])]
            team_name = th_cells[0] if th_cells else "Unknown"

            for r in table.find_all("tr")[1:]:
                tds = r.find_all("td")
                if len(tds) < 5:
                    continue

                p_link = r.find("a", href=re.compile(r"/player/(\d+)/"))
                p_id = int(re.search(r"/player/(\d+)/", p_link["href"]).group(1)) if p_link else None

                nick_elem = r.find(class_="player-nick")
                player_nick = nick_elem.text.strip() if nick_elem else (p_link.text.strip() if p_link else "Unknown")

                # K-D: e.g. "24-16"
                kd_text = tds[1].text.strip()
                kd_parts = kd_text.split("-")
                kills = int(kd_parts[0]) if len(kd_parts) == 2 and kd_parts[0].isdigit() else 0
                deaths = int(kd_parts[1]) if len(kd_parts) == 2 and kd_parts[1].isdigit() else 0
                plus_minus = kills - deaths

                # ADR (usually -5 or -4)
                adr_text = tds[-5].text.strip()
                adr = float(adr_text) if adr_text.replace(".", "", 1).isdigit() else None

                # KAST% (usually -3 or -2)
                kast_text = tds[-3].text.strip().replace("%", "")
                kast = float(kast_text) if kast_text.replace(".", "", 1).isdigit() else None

                # Rating (last cell)
                rating_text = tds[-1].text.strip()
                rating = float(rating_text) if rating_text.replace(".", "", 1).isdigit() else None

                # Round Swing (Rating 3.0 / HLTV round swing percentage, e.g. "+1.62%", "-2.26%")
                swing_td = r.find("td", class_=lambda c: c and "roundSwing" in c)
                round_swing = None
                if swing_td:
                    s_clean = swing_td.text.strip().replace("%", "")
                    try:
                        round_swing = float(s_clean)
                    except ValueError:
                        round_swing = None

                player_stats.append({
                    "match_id": match_id,
                    "event_id": event_id,
                    "map_name": map_name,
                    "team": team_name,
                    "player_id": p_id,
                    "player_nick": player_nick,
                    "kills": kills,
                    "deaths": deaths,
                    "plus_minus": plus_minus,
                    "adr": adr,
                    "kast_pct": kast,
                    "rating": rating,
                    "round_swing": round_swing,
                })

    return {
        "map_results": map_results,
        "player_stats": player_stats,
    }


def extract_stats_for_match(match_id: int, event_id: int) -> int:
    """Extract and persist HLTV stats from a cached match HTML file."""
    cache_path = MATCHES_CACHE_DIR / f"match_{match_id}.html"
    if not cache_path.exists():
        logger.warning("Cache file for match %d not found at %s", match_id, cache_path)
        return 0

    html = cache_path.read_text(encoding="utf-8")
    parsed = parse_hltv_match_stats(html, match_id=match_id, event_id=event_id)

    with get_db() as conn:
        for mr in parsed["map_results"]:
            upsert_hltv_map_stat(conn, mr)
        for ps in parsed["player_stats"]:
            upsert_hltv_player_stat(conn, ps)
        conn.commit()

    return len(parsed["player_stats"])


def extract_stats_for_event(event_id: int) -> None:
    """Extract HLTV stats for all cached matches belonging to an event."""
    with get_db() as conn:
        matches = conn.execute(
            "SELECT match_id, event_id, team1, team2 FROM matches WHERE event_id = ?",
            (event_id,),
        ).fetchall()

    logger.info("Extracting HLTV stats for %d matches in event %d...", len(matches), event_id)
    total_stats_rows = 0

    for m in matches:
        count = extract_stats_for_match(m["match_id"], m["event_id"])
        total_stats_rows += count
        logger.info("Match %d (%s vs %s): extracted %d player stats records",
                    m["match_id"], m["team1"], m["team2"], count)

    logger.info("[COMPLETE] Extracted %d total HLTV player stats records for event %d.",
                total_stats_rows, event_id)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Extract HLTV stats from cached match HTML files.")
    parser.add_argument("--event", type=int, default=6865, help="Event ID (default: 6865 for IEM Sydney 2023)")
    parser.add_argument("--match", type=int, default=None, help="Optional single match ID")
    args = parser.parse_args()

    if args.match:
        with get_db() as conn:
            m = conn.execute("SELECT event_id FROM matches WHERE match_id = ?", (args.match,)).fetchone()
            eid = m["event_id"] if m else args.event
        extract_stats_for_match(args.match, eid)
    else:
        extract_stats_for_event(args.event)
