#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import os
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCHEMA_SPEC_PATH = ROOT.parent / "specs" / "SCHEMA_SPEC.md"
SOURCES_PATH = ROOT.parent / "SOURCES.md"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import asyncpg
from dotenv import load_dotenv

from routing.migrations import ensure_alerts_retry_column

KNOWN_SOURCE_IDS = {
    "MCA OGD": "mca_ogd",
    "MCA charge register": "mca_charges",
    "MCA director/DIN data": "mca_directors",
    "MCA CDM portal": "mca_cdm",
    "ROC filings": "roc_filings",
    "e-Courts": "ecourts",
    "NCLT": "nclt",
    "DRT": "drt",
    "SARFAESI notices": "sarfaesi",
    "IBBI": "ibbi",
    "High Court commercial division": "high_court",
    "Supreme Court cause lists": "supreme_court",
    "Labour court orders": "labour_court",
    "GST portal": "gst",
    "DGFT": "dgft",
    "SEBI bulk/block deals": "sebi_bulk_deals",
    "SEBI enforcement orders": "sebi_enforcement",
    "CERSAI": "cersai",
    "RBI wilful defaulter list": "rbi_wilful_defaulter",
    "CCI filings": "cci",
    "State VAT/commercial tax portals": "state_vat",
    "GeM": "gem",
    "CPPP": "cppp",
    "EPFO": "epfo",
    "ESIC": "esic",
    "Naukri.com": "naukri",
    "Indeed India / Foundit": "indeed",
    "Company career pages": "career_pages",
    "Glassdoor India": "glassdoor_india",
    "LinkedIn": "linkedin_indirect",
    "Udyam registration portal": "udyam",
    "RERA": "rera",
    "MOEF environment clearance portal": "moef",
    "Pollution control boards": "cpcb",
    "RBI NBFC/bank notifications": "rbi_nbfc",
}


def fail(message: str) -> None:
    raise SystemExit(message)


def extract_sql_block(text: str) -> str:
    match = re.search(r"```sql\n(.*?)\n```", text, re.DOTALL)
    if not match:
        fail("Could not find SQL block in SCHEMA_SPEC.md.")
    return match.group(1)


def split_sql_statements(sql: str) -> list[str]:
    statements: list[str] = []
    current: list[str] = []
    in_dollar_quote = False
    dollar_tag = "$$"

    i = 0
    while i < len(sql):
        if sql.startswith("$$", i):
            current.append("$$")
            in_dollar_quote = not in_dollar_quote
            i += 2
            continue

        char = sql[i]
        if char == ";" and not in_dollar_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue

        current.append(char)
        i += 1

    trailing = "".join(current).strip()
    if trailing:
        statements.append(trailing)
    return statements


def make_idempotent(statement: str) -> str:
    stripped = statement.strip()

    if stripped.startswith("CREATE TABLE "):
        return stripped.replace("CREATE TABLE ", "CREATE TABLE IF NOT EXISTS ", 1)
    if stripped.startswith("CREATE UNIQUE INDEX "):
        return stripped.replace("CREATE UNIQUE INDEX ", "CREATE UNIQUE INDEX IF NOT EXISTS ", 1)
    if stripped.startswith("CREATE INDEX "):
        return stripped.replace("CREATE INDEX ", "CREATE INDEX IF NOT EXISTS ", 1)
    if stripped.startswith("CREATE TRIGGER "):
        trigger_match = re.match(r"CREATE TRIGGER\s+(\S+)\s+(.*)", stripped, re.DOTALL)
        if not trigger_match:
            return stripped
        trigger_name = trigger_match.group(1)
        remainder = trigger_match.group(2)
        table_match = re.search(r"\bON\s+(\S+)", remainder)
        if not table_match:
            return stripped
        table_name = table_match.group(1)
        return (
            "DO $$\n"
            "BEGIN\n"
            "  IF NOT EXISTS (\n"
            "    SELECT 1\n"
            "    FROM pg_trigger\n"
            f"    WHERE tgname = '{trigger_name}'\n"
            "      AND NOT tgisinternal\n"
            "  ) THEN\n"
            f"    CREATE TRIGGER {trigger_name} {remainder};\n"
            "  END IF;\n"
            "END$$"
        )
    return stripped


def ordered_statements() -> list[str]:
    sql = extract_sql_block(SCHEMA_SPEC_PATH.read_text())
    raw_statements = split_sql_statements(sql)
    return [make_idempotent(statement) for statement in raw_statements]


def normalize_source_title(title: str) -> str:
    name = re.sub(r"^\d+\.\s*", "", title).strip()
    name = re.sub(r"\s*\(.*?\)\s*$", "", name).strip()
    name = re.sub(r"\s*\(targeted 500\)\s*$", "", name).strip()
    if name in KNOWN_SOURCE_IDS:
        return KNOWN_SOURCE_IDS[name]
    slug = name.lower()
    slug = slug.replace("&", "and")
    slug = re.sub(r"[^a-z0-9]+", "_", slug).strip("_")
    return slug


def extract_source_ids() -> list[str]:
    source_ids: list[str] = []
    for line in SOURCES_PATH.read_text().splitlines():
        if not line.startswith("### "):
            continue
        title = line[4:].strip()
        source_ids.append(normalize_source_title(title))
    return source_ids


async def apply_schema(conn: asyncpg.Connection) -> None:
    for statement in ordered_statements():
        await conn.execute(statement)


async def seed_source_state(conn: asyncpg.Connection, source_ids: list[str]) -> int:
    inserted = 0
    for source_id in source_ids:
        status = await conn.execute(
            """
            INSERT INTO source_state (source_id, status)
            VALUES ($1, 'OK')
            ON CONFLICT (source_id) DO NOTHING
            """,
            source_id,
        )
        if status.endswith("1"):
            inserted += 1
    return inserted


async def main() -> None:
    load_dotenv()
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        fail("DATABASE_URL environment variable is required.")

    source_ids = extract_source_ids()
    conn = await asyncpg.connect(database_url)
    try:
        await apply_schema(conn)
        await ensure_alerts_retry_column(conn)
        seeded = await seed_source_state(conn, source_ids)
    finally:
        await conn.close()

    print("Schema applied.")
    print(f"Seeded {seeded} source_state rows.")


if __name__ == "__main__":
    asyncio.run(main())
