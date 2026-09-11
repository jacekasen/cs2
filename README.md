# CS2 Professional Match Demo Pipeline

An automated, ultra-defensive data pipeline for discovering, cataloging, downloading, and extracting Counter-Strike 2 (CS2) professional match demo files (.dem) from HLTV.

The pipeline is designed with Cloudflare resilience, zero-redundancy disk caching, circuit breaker protection, and polite human-like jitter to safely crawl professional matches starting from the release of CS2 (October 2023) to the present.

---

## Architecture

```mermaid
flowchart TD
    subgraph Discovery ["1. Discovery & Scraping"]
        A["HLTV MVP CS2 Events<br/>(65 Tournaments)"] -->|Cautious Probe| B[Events Harvester]
        B -->|Paginated & Chronological| C[("data/catalog/mvp_events.json")]
        C -->|Select Event ID| D[Match Crawler]
        D -->|Parse Results| E[Match Pages]
        E -->|Resolve GOTV Link| F[Demo Target Queue]
    end

    subgraph Catalog ["2. State Management"]
        F --> G[("SQLite Database<br/>cs2_pro_demos.sqlite")]
    end

    subgraph Defense ["3. Cloudflare & Bot Mitigation"]
        H["Google Chrome CDP<br/>(One-time Handshake)"] -->|cf_clearance token| I[Session Cache]
        I --> J["curl_cffi Client<br/>(Chrome TLS/HTTP2 Impersonation)"]
        J -->|5.0s - 9.0s Jitter| D
        J -->|Circuit Breaker (Instant Stop)| K["Protected IP"]
    end

    subgraph Processing ["4. Ingestion & Unpacking"]
        G -->|Status: DISCOVERED| L[Streaming Downloader]
        L --> M["data/archives/<br/>*.rar"]
        M -->|7zz Unpack| N["data/demos/<br/>*.dem"]
        N -->|Verify Magic: PBDEMS2| O[Downstream CS2 Parsers<br/>demoparser2 / awpy]
    end
```

---

## Project Structure

```text
/Users/jankasen/dev/cs2/
|-- README.md
|-- data/
|   |-- cache/
|   |   |-- events/               # Raw HTML cache for event results pages
|   |   |-- matches/              # Raw HTML cache for individual match pages
|   |   \-- session_cookies.json  # Stored cf_clearance & browser fingerprint
|   \-- catalog/
|       |-- mvp_events.json       # All 65 CS2 MVP events (chronological order)
|       \-- cs2_pro_demos.sqlite  # SQLite database tracking events, matches, and demos
\-- src/
    |-- config.py                 # Paths, rate limits, jitter settings, browser headers
    |-- models.py                 # Data models: MVPEvent, Match
    |-- db.py                     # SQLite connection, schema, and upsert helpers
    |-- utils/
    |   |-- cache.py              # DiskCache (ensures 0 redundant network calls)
    |   |-- client.py             # StealthHLTVClient with TLS impersonation & circuit breaker
    |   \-- session_manager.py    # Automated Chrome CDP Cloudflare clearance solver
    \-- scraper/
        |-- harvest_events.py     # Discovers and paginates all CS2 MVP events
        \-- harvest_matches.py    # Crawls event matches and resolves GOTV demo links
```

---

## Anti-Bot and Cloudflare Defenses

HLTV guards its match pages and demo downloads behind Cloudflare bot management. This pipeline implements a 4-layer defense:

1. **Automated CDP Clearance (`session_manager.py`)**:
   * Uses your installed Google Chrome via Chrome DevTools Protocol (CDP) to clear Cloudflare's interactive Turnstile challenge once.
   * Extracts the resulting `cf_clearance` cookie and matching `User-Agent`, persisting them to disk.

2. **TLS and HTTP/2 Impersonation (`curl_cffi`)**:
   * Subsequent requests impersonate real desktop Chrome (`impersonate="chrome124"`), preventing TLS client hello (JA3/JA4) fingerprint detection.

3. **Circuit Breaker**:
   * Every incoming response is checked for Cloudflare challenge markers (`"Just a moment..."`, Turnstile tokens, or 403 status).
   * If detected, the client immediately halts execution rather than spamming requests, protecting your IP address from blocks.

4. **Polite Pacing and Disk-First Caching**:
   * Enforces a randomized 5.0s to 9.0s delay between consecutive requests.
   * Every fetched HTML page is permanently cached locally. If you re-run or inspect matches, it reads from disk with 0 HTTP requests.

---

## Getting Started

### 1. Environment Setup

The project uses the `cs2` conda environment:

```bash
conda activate cs2
```

Required packages:
* `curl_cffi` (TLS/HTTP2 impersonation)
* `beautifulsoup4` (HTML parsing)
* `websockets` (CDP communication)
* `demoparser2` and `awpy` (Downstream CS2 demo analysis)

### 2. Harvest MVP CS2 Events

Scrapes all tier-1 CS2 events from HLTV (starting with the inaugural CS2 event, IEM Sydney 2023):

```bash
python -m src.scraper.harvest_events
```

Outputs:
* `data/catalog/mvp_events.json` (65 events, 3,289+ maps)
* Populates the `events` table in SQLite.

### 3. Harvest Matches and Demo Targets for an Event

To crawl all completed matches and resolve demo download links for a tournament:

```bash
# Example: Harvest all 29 matches for IEM Sydney 2023 (Event 6865)
python -m src.scraper.harvest_matches --event 6865

# Or limit to the first 5 matches for quick testing:
python -m src.scraper.harvest_matches --event 6865 --max-matches 5
```

Outputs:
* Populates `matches` and `demos` tables in `data/catalog/cs2_pro_demos.sqlite`.
* HTML cached in `data/cache/matches/match_<id>.html`.

---

## Database Schema (cs2_pro_demos.sqlite)

### events
| Column | Type | Description |
| :--- | :--- | :--- |
| `event_id` | `INTEGER PRIMARY KEY` | HLTV Event ID (e.g. `6865`) |
| `name` | `TEXT` | Tournament name (e.g. `IEM Sydney 2023`) |
| `winner` | `TEXT` | Tournament winner (e.g. `FaZe`) |
| `maps_count` | `INTEGER` | Total maps played |
| `results_url` | `TEXT` | HLTV results URL |

### matches
| Column | Type | Description |
| :--- | :--- | :--- |
| `match_id` | `INTEGER PRIMARY KEY` | HLTV Match ID (e.g. `2367264`) |
| `event_id` | `INTEGER` | Foreign key to `events` |
| `team1` | `TEXT` | Team 1 name |
| `team2` | `TEXT` | Team 2 name |
| `score1` | `INTEGER` | Team 1 score |
| `score2` | `INTEGER` | Team 2 score |
| `format` | `TEXT` | Format (`bo1`, `bo3`, `bo5`) |
| `url` | `TEXT` | Match page URL |
| `match_date` | `TEXT` | Match timestamp |

### demos
| Column | Type | Description |
| :--- | :--- | :--- |
| `demo_id` | `INTEGER PRIMARY KEY` | HLTV GOTV demo archive ID (e.g. `82852`) |
| `match_id` | `INTEGER UNIQUE` | Foreign key to `matches` |
| `download_url` | `TEXT` | Direct download URL (`https://www.hltv.org/download/demo/{id}`) |
| `maps_json` | `TEXT` | JSON list of maps played (e.g. `["overpass", "nuke", "ancient"]`) |
| `status` | `TEXT` | Queue status (`DISCOVERED`, `DOWNLOADING`, `DOWNLOADED`, `EXTRACTED`, `FAILED`) |
| `archive_path` | `TEXT` | Local path to `.rar` archive |
| `extracted_paths_json` | `TEXT` | Local paths to extracted `.dem` files |

---

## CS2 Demo Format Note

All pro match demos downloaded through this pipeline are Counter-Strike 2 Source 2 replays. 

Extracted `.dem` files begin with the 8-byte magic header:
```text
PBDEMS2\0  (Protobuf Demo Source 2)
```
*(In contrast to legacy CS:GO demos which begin with `HL2DEMO\0`)*.
