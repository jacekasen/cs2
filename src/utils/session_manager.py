"""Automated session acquisition for HLTV using Chrome CDP."""
import asyncio
import json
import logging
import os
import subprocess
import time
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

import websockets

from src.config import CACHE_DIR, HLTV_BASE_URL

logger = logging.getLogger("cs2_pipeline.session_manager")
SESSION_FILE = CACHE_DIR / "session_cookies.json"


def load_saved_session() -> Optional[Dict[str, Any]]:
    """Load previously saved cookies and User-Agent if available."""
    if SESSION_FILE.exists():
        try:
            data = json.loads(SESSION_FILE.read_text(encoding="utf-8"))
            if "cookies" in data and "user_agent" in data:
                if "cf_clearance" in data["cookies"]:
                    return data
        except Exception as e:
            logger.warning("Failed to load existing session file: %s", e)
    return None


def acquire_cf_session(timeout: int = 90) -> Dict[str, Any]:
    """Launch Chrome with remote debugging, solve Cloudflare, and capture cookies."""
    logger.info("Launching Chrome via CDP to acquire Cloudflare clearance...")
    chrome_bin = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
    user_data_dir = "/tmp/chrome_cs2_session"
    port = 9222

    # Clean up previous session lock if any
    subprocess.run(["pkill", "-f", f"remote-debugging-port={port}"], stderr=subprocess.DEVNULL)
    time.sleep(0.5)

    cmd = [
        chrome_bin,
        f"--remote-debugging-port={port}",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        HLTV_BASE_URL,
    ]

    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        # Wait for CDP endpoint to be responsive
        start = time.time()
        ver_data = None
        while time.time() - start < timeout:
            try:
                resp = urllib.request.urlopen(f"http://127.0.0.1:{port}/json/version", timeout=1.0)
                ver_data = json.loads(resp.read().decode("utf-8"))
                break
            except Exception:
                time.sleep(0.5)

        if not ver_data:
            raise RuntimeError("Timed out waiting for Chrome CDP endpoint.")

        ua = ver_data["User-Agent"]
        browser_ws = ver_data["webSocketDebuggerUrl"]

        async def _fetch_cookies() -> Dict[str, str]:
            async with websockets.connect(browser_ws) as ws:
                # Clear existing cookies to ensure fresh token
                await ws.send(json.dumps({"id": 1, "method": "Network.clearBrowserCookies"}))
                await ws.recv()

                # Get page target
                await ws.send(json.dumps({"id": 2, "method": "Target.getTargets"}))
                res = json.loads(await ws.recv())
                targets = res.get("result", {}).get("targetInfos", [])
                page_targets = [t for t in targets if t["type"] == "page"]

                session_id = None
                if page_targets:
                    await ws.send(json.dumps({
                        "id": 3,
                        "method": "Target.attachToTarget",
                        "params": {"targetId": page_targets[0]["targetId"], "flatten": True}
                    }))
                    attach_res = json.loads(await ws.recv())
                    session_id = attach_res.get("params", {}).get("sessionId")

                logger.info(
                    "Chrome is open at %s. Please complete any verification check if prompted...",
                    HLTV_BASE_URL,
                )

                # Poll for verified clearance
                for step in range(timeout * 2):
                    page_title = ""
                    if session_id:
                        await ws.send(json.dumps({
                            "id": 100 + step,
                            "sessionId": session_id,
                            "method": "Runtime.evaluate",
                            "params": {"expression": "document.title"},
                        }))
                        while True:
                            msg = json.loads(await ws.recv())
                            if msg.get("id") == 100 + step:
                                page_title = msg.get("result", {}).get("result", {}).get("value", "")
                                break

                    # Poll cookies
                    await ws.send(json.dumps({"id": 1000 + step, "method": "Storage.getCookies"}))
                    cookies_res = json.loads(await ws.recv())
                    cookies = {
                        c["name"]: c["value"]
                        for c in cookies_res.get("result", {}).get("cookies", [])
                    }

                    if "cf_clearance" in cookies and page_title and "Just a moment" not in page_title:
                        logger.info("[SUCCESS] Acquired verified cf_clearance: %s... (Title: %s)",
                                    cookies["cf_clearance"][:16], page_title)
                        return cookies

                    if step % 10 == 0:
                        logger.info("Waiting for Cloudflare clearance... (Current title: %s)", page_title or "loading")

                    await asyncio.sleep(0.5)

                return cookies

        cookies = asyncio.run(_fetch_cookies())
        session_data = {"user_agent": ua, "cookies": cookies}
        SESSION_FILE.write_text(json.dumps(session_data, indent=2), encoding="utf-8")
        logger.info("Saved valid session to %s (cookies: %d)", SESSION_FILE, len(cookies))
        return session_data

    finally:
        subprocess.run(["pkill", "-f", f"remote-debugging-port={port}"], stderr=subprocess.DEVNULL)
        proc.kill()


def get_or_create_session(force: bool = False) -> Dict[str, Any]:
    """Return active session or acquire a new one."""
    if not force:
        saved = load_saved_session()
        if saved:
            logger.info("Reusing existing session from %s", SESSION_FILE)
            return saved

    return acquire_cf_session()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    get_or_create_session(force=True)
