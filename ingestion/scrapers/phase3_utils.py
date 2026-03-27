from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import date, datetime
from typing import Any, Optional

import psycopg2

from ingestion.entity_resolver import EntityResolver
from ingestion.vlm_extractor import route_document

logger = logging.getLogger(__name__)

DATE_FORMATS = (
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%Y-%m-%d",
    "%d %b %Y",
    "%d %B %Y",
    "%d.%m.%Y",
    "%b %d, %Y",
    "%B %d, %Y",
)

_ALLOWED_LOOKUP_COLUMNS = {"pan", "gstin", "epfo_id", "udyam_id"}


class _SyncDBWrapper:
    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=()):
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return cursor

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()


def _default_db():
    database_url = os.environ.get("DATABASE_URL", "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL environment variable is required")
    return _SyncDBWrapper(psycopg2.connect(database_url))


class BaseSignalScraper:
    source_id = ""

    def __init__(self, db_conn=None):
        self.db = db_conn or _default_db()

    def _execute(self, sql: str, params: tuple = ()):
        return self.db.execute(sql, params)

    def _fetchone(self, sql: str, params: tuple = ()):
        return self._execute(sql, params).fetchone()

    def _fetchall(self, sql: str, params: tuple = ()):
        cursor = self._execute(sql, params)
        fetchall = getattr(cursor, "fetchall", None)
        if callable(fetchall):
            return fetchall()
        row = getattr(cursor, "fetchone", lambda: None)()
        return [row] if row else []

    def _commit(self) -> None:
        commit = getattr(self.db, "commit", None)
        if callable(commit):
            commit()

    def _rollback(self) -> None:
        rollback = getattr(self.db, "rollback", None)
        if callable(rollback):
            rollback()

    def _table_has_column(self, table_name: str, column_name: str) -> bool:
        try:
            row = self._fetchone(
                """
                SELECT 1
                FROM information_schema.columns
                WHERE table_name = %s
                  AND column_name = %s
                LIMIT 1
                """,
                (table_name, column_name),
            )
            return bool(row)
        except Exception:
            return False

    def _load_state(self, key: str) -> dict:
        row = self._fetchone(
            "SELECT last_data_hash, notes FROM source_state WHERE source_id = %s",
            (key,),
        )
        if not row:
            return {}

        notes = None
        if isinstance(row, dict):
            notes = row.get("notes")
        elif len(row) > 1:
            notes = row[1]

        if not notes:
            return {}
        if isinstance(notes, dict):
            return notes
        try:
            return json.loads(notes)
        except (TypeError, json.JSONDecodeError):
            return {}

    def _store_state(self, key: str, payload: dict, record_count: int = 1, status: str = "OK") -> None:
        encoded = json.dumps(payload, sort_keys=True, default=str)
        payload_hash = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
        self._execute(
            """
            INSERT INTO source_state (
                source_id, last_pull_at, last_data_hash, record_count, status, notes, updated_at
            ) VALUES (%s, NOW(), %s, %s, %s, %s, NOW())
            ON CONFLICT (source_id) DO UPDATE
              SET last_pull_at = NOW(),
                  last_data_hash = EXCLUDED.last_data_hash,
                  record_count = EXCLUDED.record_count,
                  status = EXCLUDED.status,
                  notes = EXCLUDED.notes,
                  consecutive_failures = 0,
                  updated_at = NOW()
            """,
            (key, payload_hash, record_count, status, encoded),
        )
        self._commit()

    def _mark_broken(self, key: str, notes: str) -> None:
        self._execute(
            """
            INSERT INTO source_state (
                source_id, last_pull_at, status, notes, updated_at
            ) VALUES (%s, NOW(), 'SCRAPER_BROKEN', %s, NOW())
            ON CONFLICT (source_id) DO UPDATE
              SET last_pull_at = NOW(),
                  status = 'SCRAPER_BROKEN',
                  notes = EXCLUDED.notes,
                  updated_at = NOW()
            """,
            (key, notes),
        )
        self._commit()

    def _insert_event(
        self,
        cin: Optional[str],
        event_type: str,
        severity: str,
        payload: dict,
        *,
        source: Optional[str] = None,
    ) -> None:
        self._execute(
            """
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES (%s, %s, %s, %s, NOW(), %s)
            """,
            (
                cin,
                source or self.source_id,
                event_type,
                severity,
                json.dumps(payload, default=str),
            ),
        )
        self._commit()

    def _store_unmapped(self, identifier_value: str, raw_data: dict) -> None:
        self._execute(
            """
            INSERT INTO unmapped_signals
              (source, identifier_type, identifier_value, raw_data, detected_at)
            VALUES (%s, 'COMPANY_NAME', %s, %s, NOW())
            """,
            (self.source_id, identifier_value, json.dumps(raw_data, default=str)),
        )
        self._commit()

    def _resolve_entity(self, raw_name: str):
        return EntityResolver(self.db).resolve(raw_name)

    def _load_watchlist_companies(self, include_careers_url: bool = False) -> list[dict]:
        careers_sql = ", me.careers_url" if include_careers_url and self._table_has_column("master_entities", "careers_url") else ""
        try:
            rows = self._fetchall(
                f"""
                SELECT DISTINCT me.cin, me.company_name{careers_sql}
                FROM master_entities me
                JOIN watchlists w ON w.is_active = TRUE
                WHERE w.cin_list IS NULL OR me.cin = ANY(w.cin_list)
                ORDER BY me.cin
                """
            )
        except Exception as exc:
            logger.warning("%s: unable to load watchlist companies: %s", self.source_id, exc)
            return []

        companies: list[dict] = []
        for row in rows:
            if isinstance(row, dict):
                payload = {"cin": row.get("cin"), "company_name": row.get("company_name")}
                if include_careers_url:
                    payload["careers_url"] = row.get("careers_url")
            else:
                payload = {"cin": row[0], "company_name": row[1]}
                if include_careers_url:
                    payload["careers_url"] = row[2] if len(row) > 2 else None
            if payload["cin"] and payload["company_name"]:
                companies.append(payload)
        return companies

    def _lookup_cin_by_column(self, column: str, value: Optional[str]) -> Optional[str]:
        if not value or column not in _ALLOWED_LOOKUP_COLUMNS:
            return None
        row = self._fetchone(
            f"SELECT cin FROM master_entities WHERE {column} = %s LIMIT 1",
            (value,),
        )
        if not row:
            return None
        if isinstance(row, dict):
            return row.get("cin")
        return row[0]

    def _lookup_director_cin(self, raw_name: str) -> Optional[str]:
        if not raw_name:
            return None
        row = self._fetchone(
            """
            SELECT cin
            FROM governance_graph
            WHERE LOWER(director_name) = LOWER(%s)
            ORDER BY date_of_appointment DESC NULLS LAST
            LIMIT 1
            """,
            (raw_name.strip(),),
        )
        if not row:
            return None
        if isinstance(row, dict):
            return row.get("cin")
        return row[0]

    async def _solve_captcha_with_route(self, page, img_selector: str, input_selector: str) -> bool:
        img = page.locator(img_selector)
        if await img.count() == 0:
            return True
        raw = await img.screenshot()
        solved = route_document("captcha", raw)
        if not solved:
            return False
        await page.fill(input_selector, str(solved).strip())
        return True

    @staticmethod
    def parse_date(raw: Optional[str]) -> Optional[date]:
        if not raw:
            return None
        cleaned = " ".join(str(raw).strip().replace(",", " ").split())
        for fmt in DATE_FORMATS:
            try:
                return datetime.strptime(cleaned, fmt).date()
            except ValueError:
                continue
        match = re.search(r"(\d{4}-\d{2}-\d{2})", cleaned)
        if match:
            try:
                return datetime.strptime(match.group(1), "%Y-%m-%d").date()
            except ValueError:
                return None
        return None

    @staticmethod
    def parse_amount(raw: Optional[str]) -> Optional[int]:
        if raw in (None, ""):
            return None
        cleaned = str(raw).replace(",", "").replace("₹", "").strip()
        if not cleaned:
            return None
        lowered = cleaned.lower()
        match = re.search(r"(\d+(?:\.\d+)?)", lowered)
        if not match:
            return None
        value = float(match.group(1))
        if "crore" in lowered or re.search(r"\bcr\b", lowered):
            return int(value * 10_000_000)
        if "lakh" in lowered or "lac" in lowered:
            return int(value * 100_000)
        return int(value)

    @staticmethod
    def normalize_text(raw: Optional[str]) -> str:
        return " ".join((raw or "").split())

    @staticmethod
    def compute_digest(payload: Any) -> str:
        encoded = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @staticmethod
    def extract_json_value(payload: Any, *tokens: str) -> Optional[Any]:
        wanted = tuple(token.lower() for token in tokens)
        queue = [payload]
        while queue:
            current = queue.pop(0)
            if isinstance(current, dict):
                for key, value in current.items():
                    lowered = str(key).lower()
                    if all(token in lowered for token in wanted):
                        return value
                    queue.append(value)
            elif isinstance(current, list):
                queue.extend(current)
        return None

    @staticmethod
    async def retry_with_backoff(coro_factory, retries: int = 3, initial_delay: float = 2.0):
        delay = initial_delay
        for attempt in range(retries):
            result = await coro_factory()
            if result is not None:
                return result
            if attempt < retries - 1:
                await asyncio.sleep(delay)
                delay *= 2
        return None
