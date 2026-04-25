#!/usr/bin/env python3
"""
Initialize PostgreSQL database schema for NSW PSN API Proxy.
Run this once after creating the database to create all required tables.

Usage:
    python init_postgres.py

Requires DATABASE_URL environment variable:
    postgresql://user:password@host:port/database

Example:
    export DATABASE_URL=postgresql://nswpsn:secret@localhost:5432/nswpsn
    python init_postgres.py
"""
import os
import sys

# Load .env before importing db
from dotenv import load_dotenv
load_dotenv()

from db import get_conn


def main():
    url = os.environ.get('DATABASE_URL')
    if not url:
        print("ERROR: DATABASE_URL environment variable is required")
        print("Example: postgresql://nswpsn:password@localhost:5432/nswpsn")
        sys.exit(1)

    conn = get_conn()
    cur = conn.cursor()

    try:
        # api_data_cache
        cur.execute('''
            CREATE TABLE IF NOT EXISTS api_data_cache (
                endpoint TEXT PRIMARY KEY,
                data TEXT NOT NULL,
                timestamp BIGINT NOT NULL,
                ttl INTEGER NOT NULL,
                fetch_time_ms INTEGER DEFAULT 0
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_cache_timestamp ON api_data_cache(timestamp)')
        print("✓ api_data_cache")

        # stats_snapshots (timestamp is unique per hour)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS stats_snapshots (
                id SERIAL PRIMARY KEY,
                timestamp BIGINT NOT NULL UNIQUE,
                data TEXT NOT NULL
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_stats_timestamp ON stats_snapshots(timestamp)')
        print("✓ stats_snapshots")

        # editor_requests
        cur.execute('''
            CREATE TABLE IF NOT EXISTS editor_requests (
                id SERIAL PRIMARY KEY,
                email TEXT NOT NULL,
                discord_id TEXT NOT NULL,
                website TEXT,
                about TEXT,
                request_type TEXT,
                region TEXT,
                background TEXT,
                background_details TEXT,
                status TEXT DEFAULT 'pending',
                created_at BIGINT NOT NULL,
                reviewed_at BIGINT,
                notes TEXT,
                has_existing_setup TEXT,
                setup_details TEXT,
                tech_experience TEXT,
                experience_level INTEGER
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_editor_requests_status ON editor_requests(status)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_editor_requests_email ON editor_requests(email)')
        print("✓ editor_requests")

        # data_history (single table for all sources - PostgreSQL handles concurrency)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS data_history (
                id SERIAL PRIMARY KEY,
                source TEXT NOT NULL,
                source_id TEXT,
                source_provider TEXT,
                source_type TEXT,
                fetched_at BIGINT NOT NULL,
                source_timestamp TEXT,
                source_timestamp_unix BIGINT,
                latitude DOUBLE PRECISION,
                longitude DOUBLE PRECISION,
                location_text TEXT,
                title TEXT,
                category TEXT,
                subcategory TEXT,
                status TEXT,
                severity TEXT,
                data TEXT NOT NULL,
                is_active INTEGER DEFAULT 1,
                is_live INTEGER DEFAULT 1,
                last_seen BIGINT,
                data_hash TEXT,
                is_latest INTEGER DEFAULT 1
            )
        ''')
        for idx in [
            'CREATE INDEX IF NOT EXISTS idx_data_source ON data_history(source)',
            'CREATE INDEX IF NOT EXISTS idx_data_source_id ON data_history(source_id)',
            'CREATE INDEX IF NOT EXISTS idx_data_fetched ON data_history(fetched_at)',
            'CREATE INDEX IF NOT EXISTS idx_data_source_ts ON data_history(source_timestamp_unix)',
            'CREATE INDEX IF NOT EXISTS idx_data_location ON data_history(latitude, longitude)',
            'CREATE INDEX IF NOT EXISTS idx_data_category ON data_history(category)',
            'CREATE INDEX IF NOT EXISTS idx_data_active ON data_history(is_active)',
            'CREATE INDEX IF NOT EXISTS idx_data_live ON data_history(is_live)',
            'CREATE INDEX IF NOT EXISTS idx_data_source_fetched ON data_history(source, fetched_at)',
            'CREATE INDEX IF NOT EXISTS idx_data_hash ON data_history(source, source_id, data_hash)',
            'CREATE INDEX IF NOT EXISTS idx_data_unique ON data_history(source, source_id, fetched_at)',
            'CREATE INDEX IF NOT EXISTS idx_data_live_fetched ON data_history(is_live, fetched_at)',
            'CREATE INDEX IF NOT EXISTS idx_data_latest ON data_history(is_latest, fetched_at DESC)',
            'CREATE INDEX IF NOT EXISTS idx_data_latest_live ON data_history(is_latest, is_live)',
            'CREATE INDEX IF NOT EXISTS idx_data_provider ON data_history(source_provider)',
            'CREATE INDEX IF NOT EXISTS idx_data_provider_type ON data_history(source_provider, source_type)',
            'CREATE INDEX IF NOT EXISTS idx_data_provider_latest ON data_history(source_provider, is_latest)',
            # Partial indexes — the logs.html page always queries with
            # `unique=1` (is_latest=1), so the planner needs a small, unambiguous
            # index for that subset. Non-partial idx_data_latest has ~5% selectivity
            # and the planner sometimes picks fetched_at range scans instead.
            'CREATE INDEX IF NOT EXISTS idx_data_latest_only_fetched ON data_history(fetched_at DESC) WHERE is_latest = 1',
            'CREATE INDEX IF NOT EXISTS idx_data_latest_only_source ON data_history(source, fetched_at DESC) WHERE is_latest = 1',
        ]:
            cur.execute(idx)
        print("✓ data_history")

        # data_history_filter_cache — pre-computed filter dropdown options.
        # Rebuilt periodically (~every 5 min) from is_latest=1 rows so the
        # logs.html filters load instantly instead of triggering a full
        # GROUPING SETS scan of data_history every time.
        #
        # Schema: one row per (dimension, source-scope, value) — `kind` is
        # which dropdown ('source', 'category', 'subcategory', 'status',
        # 'severity'), `source` is the parent source for source-scoped
        # dimensions (NULL for kind='source').
        # Note: `source` is TEXT NOT NULL with '' used instead of NULL for
        # kind='source' rows (the top-level list), so the composite PK works.
        cur.execute('''
            CREATE TABLE IF NOT EXISTS data_history_filter_cache (
                kind TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT '',
                value TEXT NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                updated_at TIMESTAMPTZ DEFAULT now(),
                PRIMARY KEY (kind, source, value)
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_filter_cache_kind ON data_history_filter_cache(kind)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_filter_cache_kind_source ON data_history_filter_cache(kind, source)')
        print("✓ data_history_filter_cache")

        # incidents (user-submitted emergency incidents)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS incidents (
                id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
                title TEXT NOT NULL,
                description TEXT DEFAULT '',
                lat DOUBLE PRECISION NOT NULL,
                lng DOUBLE PRECISION NOT NULL,
                location TEXT DEFAULT '',
                type JSONB DEFAULT '[]'::jsonb,
                status TEXT DEFAULT 'Going',
                size TEXT DEFAULT '-',
                responding_agencies JSONB DEFAULT '[]'::jsonb,
                created_at TIMESTAMPTZ DEFAULT now(),
                updated_at TIMESTAMPTZ DEFAULT now(),
                expires_at TIMESTAMPTZ,
                is_rfs_stub BOOLEAN DEFAULT false
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_incidents_expires ON incidents(expires_at)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_incidents_rfs_stub ON incidents(is_rfs_stub)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_incidents_status ON incidents(status)')
        print("✓ incidents")

        # incident_updates (log entries for incidents)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS incident_updates (
                id TEXT PRIMARY KEY DEFAULT gen_random_uuid()::text,
                incident_id TEXT NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
                message TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT now()
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_incident_updates_incident ON incident_updates(incident_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_incident_updates_created ON incident_updates(created_at)')
        print("✓ incident_updates")

        # user_roles (role assignments)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS user_roles (
                id SERIAL PRIMARY KEY,
                user_id TEXT NOT NULL,
                role TEXT NOT NULL,
                granted_by TEXT DEFAULT 'system',
                request_id INTEGER,
                created_at TIMESTAMPTZ DEFAULT now(),
                UNIQUE(user_id, role)
            )
        ''')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_roles_user ON user_roles(user_id)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_user_roles_role ON user_roles(role)')
        print("✓ user_roles")

        # rdio_summaries (LLM-generated summaries of rdio-scanner transcripts)
        # hour_slot: 1-24 convention (1 = 00:00-01:00 local, 24 = 23:00-24:00 local)
        # day_date: YYYY-MM-DD for the local day (NULL for hourly unless you want to index)
        # release_at: hold a prefetched summary until this time (NULL = available immediately)
        cur.execute('''
            CREATE TABLE IF NOT EXISTS rdio_summaries (
                id SERIAL PRIMARY KEY,
                summary_type TEXT NOT NULL,
                period_start TIMESTAMPTZ NOT NULL,
                period_end TIMESTAMPTZ NOT NULL,
                day_date DATE NOT NULL,
                hour_slot INTEGER,
                summary TEXT NOT NULL,
                call_count INTEGER NOT NULL DEFAULT 0,
                transcript_chars INTEGER NOT NULL DEFAULT 0,
                model TEXT,
                details JSONB DEFAULT '{}'::jsonb,
                release_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ DEFAULT now(),
                UNIQUE(summary_type, period_start)
            )
        ''')
        # Add column on existing deployments
        cur.execute('ALTER TABLE rdio_summaries ADD COLUMN IF NOT EXISTS release_at TIMESTAMPTZ')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_rdio_summaries_type ON rdio_summaries(summary_type)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_rdio_summaries_day ON rdio_summaries(day_date)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_rdio_summaries_day_hour ON rdio_summaries(day_date, hour_slot)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_rdio_summaries_period ON rdio_summaries(period_start DESC)')
        cur.execute('CREATE INDEX IF NOT EXISTS idx_rdio_summaries_release ON rdio_summaries(release_at)')
        print("✓ rdio_summaries")

        conn.commit()
        print("\nDatabase initialized successfully.")

    except Exception as e:
        conn.rollback()
        print(f"\nERROR: {e}")
        sys.exit(1)
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    main()
