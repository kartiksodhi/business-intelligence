#!/usr/bin/env python3
"""
Migration: create entity_aliases table.
Run once on any environment where setup_db.py has already been executed.

Usage:
    DATABASE_URL=postgresql://... python3 bi_engine/scripts/migrate_entity_aliases.py
"""
from __future__ import annotations

import os
import sys

import psycopg2

DDL = """
CREATE TABLE IF NOT EXISTS entity_aliases (
  id               SERIAL        PRIMARY KEY,
  alias_name       TEXT          NOT NULL,
  normalized_alias TEXT          NOT NULL,
  cin              VARCHAR(21)   NOT NULL REFERENCES master_entities (cin),
  source           VARCHAR(50),
  created_at       TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_aliases_normalized_cin
  ON entity_aliases (normalized_alias, cin);

CREATE INDEX IF NOT EXISTS idx_entity_aliases_normalized
  ON entity_aliases (normalized_alias);
"""


def main() -> None:
    url = os.environ.get("DATABASE_URL")
    if not url:
        sys.exit("DATABASE_URL not set")
    conn = psycopg2.connect(url)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(DDL)
    conn.close()
    print("entity_aliases table created (or already exists).")


if __name__ == "__main__":
    main()
