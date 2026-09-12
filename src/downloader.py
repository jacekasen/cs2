"""Streaming demo downloader and unar extractor with CS2 Source 2 validation."""
import argparse
import json
import logging
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

from src.config import (
    ARCHIVES_DIR,
    DEMOS_DIR,
    SEVEN_ZIP_PATH,
    UNAR_PATH,
)
from src.db import get_db
from src.utils.client import StealthHLTVClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("cs2_pipeline.downloader")


def validate_cs2_demo(dem_path: Path) -> bool:
    """Verify that a .dem file has the CS2 Source 2 protobuf magic header (PBDEMS2\\0)."""
    if not dem_path.exists() or dem_path.stat().st_size < 16:
        return False
    with open(dem_path, "rb") as f:
        header = f.read(8)
    return header.startswith(b"PBDEMS2")


def download_demo_archive(demo_id: int, client: Optional[StealthHLTVClient] = None) -> Path:
    """Stream download a demo archive from HLTV/R2 with atomic write."""
    close_client_at_end = False
    if client is None:
        client = StealthHLTVClient()
        close_client_at_end = True

    try:
        with get_db() as conn:
            row = conn.execute("""
                SELECT d.demo_id, d.match_id, d.download_url, d.status,
                       m.event_id, m.team1, m.team2, m.format, m.url as match_url,
                       e.name as event_name
                FROM demos d
                JOIN matches m ON d.match_id = m.match_id
                JOIN events e ON m.event_id = e.event_id
                WHERE d.demo_id = ?
            """, (demo_id,)).fetchone()

        if not row:
            raise ValueError(f"Demo ID {demo_id} not found in database!")

        clean_t1 = "".join(c if c.isalnum() else "_" for c in row["team1"]).strip("_")
        clean_t2 = "".join(c if c.isalnum() else "_" for c in row["team2"]).strip("_")
        filename = f"{row['event_id']}_{row['match_id']}_{demo_id}_{clean_t1}_vs_{clean_t2}.rar"
        archive_path = ARCHIVES_DIR / filename
        part_path = ARCHIVES_DIR / f"{filename}.part"

        if archive_path.exists() and archive_path.stat().st_size > 0:
            logger.info("Archive already exists on disk: %s (%.1f MB)",
                        archive_path.name, archive_path.stat().st_size / (1024 * 1024))
            with get_db() as conn:
                conn.execute("UPDATE demos SET status = 'DOWNLOADED', archive_path = ? WHERE demo_id = ?",
                             (str(archive_path), demo_id))
                conn.commit()
            return archive_path

        logger.info("Starting download for demo %d (%s vs %s, %s)...",
                    demo_id, row["team1"], row["team2"], row["format"])

        with get_db() as conn:
            conn.execute("UPDATE demos SET status = 'DOWNLOADING', updated_at = CURRENT_TIMESTAMP WHERE demo_id = ?",
                         (demo_id,))
            conn.commit()

        headers = dict(client._session.headers)
        headers["Referer"] = row["match_url"]
        headers["User-Agent"] = client.user_agent

        start_time = time.time()
        resp = client._session.get(
            row["download_url"],
            headers=headers,
            stream=True,
            allow_redirects=True,
            timeout=30,
        )

        if resp.status_code in (403, 503):
            logger.warning(
                "Demo %d download returned HTTP %d. Refreshing Cloudflare session via CDP...",
                demo_id,
                resp.status_code,
            )
            client._init_session(force=True)
            time.sleep(2.0)
            headers = dict(client._session.headers)
            headers["Referer"] = row["match_url"]
            headers["User-Agent"] = client.user_agent
            resp = client._session.get(
                row["download_url"],
                headers=headers,
                stream=True,
                allow_redirects=True,
                timeout=30,
            )

        if resp.status_code != 200:
            err_msg = f"Failed to download demo {demo_id}: HTTP {resp.status_code}"
            logger.error(err_msg)
            with get_db() as conn:
                conn.execute("UPDATE demos SET status = 'FAILED', error_message = ? WHERE demo_id = ?",
                             (err_msg, demo_id))
                conn.commit()
            raise RuntimeError(err_msg)

        total_size = int(resp.headers.get("content-length", 0))
        downloaded = 0
        chunk_size = 2 * 1024 * 1024  # 2MB chunks
        last_log_time = time.time()

        with open(part_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)

                    now = time.time()
                    if now - last_log_time >= 3.0 or downloaded == total_size:
                        speed_mb = (downloaded / (1024 * 1024)) / max(0.1, (now - start_time))
                        if total_size > 0:
                            pct = (downloaded / total_size) * 100
                            logger.info("Downloading: %.1f%% (%.1f / %.1f MB at %.2f MB/s)",
                                        pct, downloaded / (1024 * 1024), total_size / (1024 * 1024), speed_mb)
                        else:
                            logger.info("Downloading: %.1f MB at %.2f MB/s", downloaded / (1024 * 1024), speed_mb)
                        last_log_time = now

        part_path.rename(archive_path)
        elapsed = time.time() - start_time
        final_size_mb = archive_path.stat().st_size / (1024 * 1024)
        logger.info("[COMPLETE] Downloaded %s (%.1f MB in %.1fs, avg %.2f MB/s)",
                    archive_path.name, final_size_mb, elapsed, final_size_mb / max(0.1, elapsed))

        with get_db() as conn:
            conn.execute(
                "UPDATE demos SET status = 'DOWNLOADED', archive_path = ?, updated_at = CURRENT_TIMESTAMP WHERE demo_id = ?",
                (str(archive_path), demo_id),
            )
            conn.commit()

        return archive_path

    finally:
        if close_client_at_end:
            client.close()


def extract_demo_archive(demo_id: int, archive_path: Path, keep_archive: bool = True) -> List[Path]:
    """Unpack .rar archive via unar/7zz and validate CS2 demo files."""
    with get_db() as conn:
        row = conn.execute("""
            SELECT d.demo_id, d.match_id,
                   m.event_id, m.team1, m.team2, m.format, m.match_date,
                   e.name as event_name
            FROM demos d
            JOIN matches m ON d.match_id = m.match_id
            JOIN events e ON m.event_id = e.event_id
            WHERE d.demo_id = ?
        """, (demo_id,)).fetchone()

    clean_event = "".join(c if c.isalnum() else "_" for c in row["event_name"]).strip("_")
    clean_t1 = "".join(c if c.isalnum() else "_" for c in row["team1"]).strip("_")
    clean_t2 = "".join(c if c.isalnum() else "_" for c in row["team2"]).strip("_")

    year = "2023"
    if row["match_date"]:
        year = row["match_date"].split("-")[0]

    extract_dir = DEMOS_DIR / year / clean_event / f"match_{row['match_id']}_{clean_t1}_vs_{clean_t2}"
    extract_dir.mkdir(parents=True, exist_ok=True)

    logger.info("Extracting %s into %s...", archive_path.name, extract_dir)

    # Prefer unar for full RAR5 support
    if Path(UNAR_PATH).exists():
        cmd = [UNAR_PATH, "-o", str(extract_dir), "-f", "-D", str(archive_path)]
    else:
        cmd = [SEVEN_ZIP_PATH, "x", str(archive_path), f"-o{extract_dir}", "-y"]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        err_msg = f"Extraction error: {result.stderr.strip()}"
        logger.error(err_msg)
        with get_db() as conn:
            conn.execute("UPDATE demos SET status = 'FAILED', error_message = ? WHERE demo_id = ?",
                         (err_msg, demo_id))
            conn.commit()
        raise RuntimeError(err_msg)

    dem_files = list(extract_dir.rglob("*.dem"))
    logger.info("Successfully extracted %d .dem files:", len(dem_files))

    for df in dem_files:
        is_cs2 = validate_cs2_demo(df)
        size_mb = df.stat().st_size / (1024 * 1024)
        tag = "[CS2 Source 2 Verified]" if is_cs2 else "[Non-CS2 Header!]"
        logger.info("  - %s (%.1f MB) %s", df.name, size_mb, tag)

    with get_db() as conn:
        conn.execute("""
            UPDATE demos
            SET status = 'EXTRACTED',
                extracted_paths_json = ?,
                updated_at = CURRENT_TIMESTAMP
            WHERE demo_id = ?
        """, (json.dumps([str(p) for p in dem_files]), demo_id))
        conn.commit()

    if not keep_archive:
        archive_path.unlink()
        logger.info("Removed archive %s to preserve disk space.", archive_path.name)

    return dem_files


def run_demo_pipeline(demo_id: int, keep_archive: bool = True) -> List[Path]:
    """Execute end-to-end download and extraction for a single demo ID."""
    archive_path = download_demo_archive(demo_id)
    dem_files = extract_demo_archive(demo_id, archive_path, keep_archive=keep_archive)
    return dem_files


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download and extract CS2 pro match demo archives.")
    parser.add_argument("--demo", type=int, required=True, help="Demo ID to download and unpack")
    parser.add_argument("--delete-archive", action="store_true", help="Delete the .rar archive after extraction")
    args = parser.parse_args()

    run_demo_pipeline(demo_id=args.demo, keep_archive=not args.delete_archive)
