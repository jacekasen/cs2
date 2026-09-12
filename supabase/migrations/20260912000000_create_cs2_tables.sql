-- CS2 Professional Match Demo Pipeline - Supabase PostgreSQL Schema
-- Tables: cs2_events, cs2_matches, cs2_demos, cs2_player_stats, cs2_map_stats

-- 1. Events Table
CREATE TABLE IF NOT EXISTS public.cs2_events (
    event_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    winner TEXT,
    maps_count INTEGER,
    results_url TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 2. Matches Table
CREATE TABLE IF NOT EXISTS public.cs2_matches (
    match_id INTEGER PRIMARY KEY,
    event_id INTEGER NOT NULL REFERENCES public.cs2_events (event_id),
    team1 TEXT NOT NULL,
    team2 TEXT NOT NULL,
    score1 INTEGER,
    score2 INTEGER,
    format TEXT,
    url TEXT NOT NULL,
    match_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

-- 3. Demos Table
CREATE TABLE IF NOT EXISTS public.cs2_demos (
    demo_id INTEGER PRIMARY KEY,
    match_id INTEGER UNIQUE NOT NULL REFERENCES public.cs2_matches (match_id),
    download_url TEXT NOT NULL,
    maps_json JSONB,
    status TEXT DEFAULT 'DISCOVERED',
    archive_path TEXT,
    extracted_paths_json JSONB,
    error_message TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- 4. Player Stats Table (Map-Level Boxscores)
CREATE TABLE IF NOT EXISTS public.cs2_player_stats (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES public.cs2_matches (match_id),
    event_id INTEGER NOT NULL REFERENCES public.cs2_events (event_id),
    map_name TEXT NOT NULL,
    team TEXT NOT NULL,
    player_id INTEGER,
    player_nick TEXT NOT NULL,
    kills INTEGER NOT NULL,
    deaths INTEGER NOT NULL,
    plus_minus INTEGER,
    adr DOUBLE PRECISION,
    kast_pct DOUBLE PRECISION,
    rating DOUBLE PRECISION,
    round_swing DOUBLE PRECISION,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (match_id, map_name, player_id)
);

-- 5. Map Stats Table (Map Results & Scores)
CREATE TABLE IF NOT EXISTS public.cs2_map_stats (
    id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    match_id INTEGER NOT NULL REFERENCES public.cs2_matches (match_id),
    event_id INTEGER NOT NULL REFERENCES public.cs2_events (event_id),
    map_name TEXT NOT NULL,
    team1 TEXT NOT NULL,
    team2 TEXT NOT NULL,
    score1 INTEGER NOT NULL,
    score2 INTEGER NOT NULL,
    half_scores TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (match_id, map_name)
);

-- Enable Row Level Security
ALTER TABLE public.cs2_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cs2_matches ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cs2_demos ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cs2_player_stats ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.cs2_map_stats ENABLE ROW LEVEL SECURITY;

-- Public Read Policies (for Next.js / frontend access)
DROP POLICY IF EXISTS cs2_events_public_read ON public.cs2_events;
CREATE POLICY cs2_events_public_read ON public.cs2_events FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS cs2_matches_public_read ON public.cs2_matches;
CREATE POLICY cs2_matches_public_read ON public.cs2_matches FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS cs2_demos_public_read ON public.cs2_demos;
CREATE POLICY cs2_demos_public_read ON public.cs2_demos FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS cs2_player_stats_public_read ON public.cs2_player_stats;
CREATE POLICY cs2_player_stats_public_read ON public.cs2_player_stats FOR SELECT TO anon USING (true);

DROP POLICY IF EXISTS cs2_map_stats_public_read ON public.cs2_map_stats;
CREATE POLICY cs2_map_stats_public_read ON public.cs2_map_stats FOR SELECT TO anon USING (true);

-- Indexes for fast queries
CREATE INDEX IF NOT EXISTS idx_cs2_matches_event ON public.cs2_matches (event_id);
CREATE INDEX IF NOT EXISTS idx_cs2_matches_date ON public.cs2_matches (match_date);
CREATE INDEX IF NOT EXISTS idx_cs2_demos_status ON public.cs2_demos (status);
CREATE INDEX IF NOT EXISTS idx_cs2_player_stats_player ON public.cs2_player_stats (player_id);
CREATE INDEX IF NOT EXISTS idx_cs2_player_stats_nick ON public.cs2_player_stats (player_nick);
CREATE INDEX IF NOT EXISTS idx_cs2_player_stats_match ON public.cs2_player_stats (match_id);
CREATE INDEX IF NOT EXISTS idx_cs2_player_stats_rating ON public.cs2_player_stats (rating DESC NULLS LAST);
CREATE INDEX IF NOT EXISTS idx_cs2_map_stats_match ON public.cs2_map_stats (match_id);
