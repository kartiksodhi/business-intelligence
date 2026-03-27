from __future__ import annotations

import asyncio
import io
import logging
import re
from typing import Optional

import requests

try:
    from pypdf import PdfReader
except ImportError:  # pragma: no cover - dependency availability differs by deploy target
    PdfReader = None

from .phase3_utils import BaseSignalScraper

logger = logging.getLogger(__name__)

MCA_DIRECTORS_API_URL = "https://efiling.mca.gov.in/efiling/ApiCall/getDirectorsList?cinList={cin}"
MCA_SIGNATORIES_PDF_URL = "https://www.mca.gov.in/bin/ebook/dms/getdocument?doc=SignatoriesDoc~{cin}.pdf"


class MCADirectorsScraper(BaseSignalScraper):
    source_id = "mca_directors"
    cadence_hours = 24 * 30

    async def run(self) -> list[dict]:
        rows = self._load_target_companies()
        if not rows:
            rows = self._fetchall(
                """
                SELECT cin, company_name
                FROM master_entities
                WHERE health_score IS NOT NULL
                ORDER BY health_score ASC
                LIMIT 200
                """
            )

        emitted: list[dict] = []
        for row in rows:
            cin = row["cin"] if isinstance(row, dict) else row[0]
            company_name = row.get("company_name") if isinstance(row, dict) else row[1]
            if not cin:
                continue
            emitted.extend(await self.refresh_cin(cin, company_name=company_name))
            await asyncio.sleep(2)
        self._store_state(
            self.source_id,
            {"processed_companies": len(rows), "emitted_events": len(emitted)},
            record_count=len(rows),
        )
        return emitted

    def _load_target_companies(self) -> list[dict]:
        try:
            return self._fetchall(
                """
                SELECT DISTINCT me.cin, me.company_name
                FROM watchlist_entries we
                JOIN master_entities me ON me.cin = we.cin
                LIMIT 500
                """
            )
        except Exception:
            self._rollback()
            return self._load_watchlist_companies()

    async def refresh_cin(self, cin: str, company_name: Optional[str] = None) -> list[dict]:
        directors = self._fetch_directors_v3_api(cin)
        if not directors:
            directors = self._fetch_directors_pdf(cin)

        state_key = f"mca_director:{cin}:last_refresh"
        previous = self._load_state(state_key)
        previous_directors = previous.get("directors", [])
        previous_by_key = {self._director_key(row): row for row in previous_directors if self._director_key(row)}
        previous_auditor = self._latest_role_record(previous_directors, "auditor")

        emitted: list[dict] = []
        seen_payloads: set[str] = set()

        for director in directors:
            director["cin"] = cin
            key = self._director_key(director)
            previous_director = previous_by_key.get(key)
            board_count = self._count_active_boards(director.get("din"), cin)

            for event_type, severity in self.classify_change(previous_director, director, board_count=board_count):
                payload = {"cin": cin, "company_name": company_name, **director, "board_count": board_count}
                digest = self.compute_digest([event_type, payload])
                if digest in seen_payloads:
                    continue
                self._insert_event(cin, event_type, severity, payload)
                emitted.append(payload)
                seen_payloads.add(digest)

            if self._is_role(director, "auditor") and previous_auditor and previous_auditor.get("director_name") != director.get(
                "director_name"
            ):
                for event_type, severity in self.classify_change(previous_auditor, director, board_count=board_count):
                    if event_type != "AUDITOR_CHANGED":
                        continue
                    payload = {"cin": cin, "company_name": company_name, **director, "previous_auditor": previous_auditor}
                    digest = self.compute_digest([event_type, payload])
                    if digest in seen_payloads:
                        continue
                    self._insert_event(cin, event_type, severity, payload)
                    emitted.append(payload)
                    seen_payloads.add(digest)

            self._upsert_governance_graph(cin, director)

        self._store_state(state_key, {"cin": cin, "company_name": company_name, "directors": directors}, record_count=len(directors))
        logger.info("mca_directors: fetched %d directors for cin=%s", len(directors), cin)
        return emitted

    def _fetch_directors_v3_api(self, cin: str) -> list[dict]:
        try:
            response = requests.get(
                MCA_DIRECTORS_API_URL.format(cin=cin),
                timeout=30,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json,text/plain,*/*"},
            )
            if response.status_code >= 400:
                logger.warning("mca_directors: V3 API returned %s for cin=%s", response.status_code, cin)
                return []
            payload = response.json()
        except Exception as exc:
            logger.warning("mca_directors: V3 API failed for cin=%s: %s", cin, exc)
            return []

        rows = self._extract_director_rows(payload)
        directors = [self._normalise_director_row(row) for row in rows]
        return [row for row in directors if row.get("director_name")]

    def _fetch_directors_pdf(self, cin: str) -> list[dict]:
        if PdfReader is None:
            logger.warning("mca_directors: pypdf not installed; skipping PDF fallback for cin=%s", cin)
            return []
        try:
            response = requests.get(
                MCA_SIGNATORIES_PDF_URL.format(cin=cin),
                timeout=45,
                headers={"User-Agent": "Mozilla/5.0", "Accept": "application/pdf,*/*"},
            )
            if response.status_code >= 400 or "pdf" not in (response.headers.get("content-type") or "").lower():
                return []
            reader = PdfReader(io.BytesIO(response.content))
            text = "\n".join(page.extract_text() or "" for page in reader.pages)
        except Exception as exc:
            logger.warning("mca_directors: PDF fallback failed for cin=%s: %s", cin, exc)
            return []
        return self._parse_directors_from_pdf_text(text)

    def _extract_director_rows(self, payload) -> list[dict]:
        rows: list[dict] = []
        queue = [payload]
        while queue:
            current = queue.pop(0)
            if isinstance(current, list):
                if current and all(isinstance(item, dict) for item in current):
                    if any(self._looks_like_director_row(item) for item in current):
                        rows.extend(item for item in current if isinstance(item, dict))
                        continue
                queue.extend(current)
            elif isinstance(current, dict):
                queue.extend(current.values())
        return rows

    def _looks_like_director_row(self, row: dict) -> bool:
        keys = {str(key).lower() for key in row.keys()}
        return any("director" in key or key == "din" for key in keys) or any("designation" in key for key in keys)

    def _normalise_director_row(self, row: dict) -> dict:
        director_name = self._pick_value(row, "directorname", "name", "signatoryname", "fullname")
        din = self._pick_value(row, "din", "directorid", "director_din")
        designation = self._pick_value(row, "designation", "role", "directorrole", "position")
        appointment_date = self._pick_value(
            row, "dateofappointment", "appointmentdate", "date_of_appointment", "appointment"
        )
        cessation_date = self._pick_value(
            row, "dateofcessation", "cessationdate", "date_of_cessation", "cessation"
        )
        return {
            "din": self._normalise_din(din),
            "director_name": self.normalize_text(str(director_name or "")) or None,
            "designation": self.normalize_text(str(designation or "")) or None,
            "date_of_appointment": self.parse_date(str(appointment_date or "")),
            "cessation_date": self.parse_date(str(cessation_date or "")),
            "is_active": not bool(self.parse_date(str(cessation_date or ""))),
            "raw_row": row,
        }

    def _parse_directors_from_pdf_text(self, text: str) -> list[dict]:
        directors: list[dict] = []
        seen: set[str] = set()
        pattern = re.compile(
            r"(?P<din>\d{6,8})\s+"
            r"(?P<name>[A-Z][A-Z .,&()'/-]{3,}?)\s+"
            r"(?P<designation>DIRECTOR|ADDITIONAL DIRECTOR|MANAGING DIRECTOR|WHOLE TIME DIRECTOR|NOMINEE DIRECTOR|"
            r"CHIEF FINANCIAL OFFICER|CFO|CEO|SECRETARY|AUDITOR|SIGNATORY)"
            r"(?:\s+(?P<appointment>\d{1,2}[/-]\d{1,2}[/-]\d{2,4}))?"
            r"(?:\s+(?P<cessation>\d{1,2}[/-]\d{1,2}[/-]\d{2,4}|-))?",
            re.IGNORECASE,
        )
        for raw_line in text.splitlines():
            line = self.normalize_text(raw_line)
            if not line:
                continue
            match = pattern.search(line.upper())
            if not match:
                continue
            director = {
                "din": self._normalise_din(match.group("din")),
                "director_name": self.normalize_text(match.group("name").title()),
                "designation": self.normalize_text(match.group("designation").title()),
                "date_of_appointment": self.parse_date(match.group("appointment") or ""),
                "cessation_date": self.parse_date(match.group("cessation") or ""),
            }
            director["is_active"] = not bool(director["cessation_date"])
            key = self._director_key(director)
            if key and key not in seen:
                seen.add(key)
                directors.append(director)
        return directors

    def _director_key(self, director: dict) -> str:
        din = self._normalise_din(director.get("din"))
        if din:
            return din
        return self.compute_digest(
            [
                self.normalize_text(str(director.get("director_name") or "")).lower(),
                self.normalize_text(str(director.get("designation") or "")).lower(),
            ]
        )

    def _latest_role_record(self, directors: list[dict], role: str) -> Optional[dict]:
        candidates = [row for row in directors if self._is_role(row, role)]
        if not candidates:
            return None
        return max(candidates, key=lambda row: row.get("date_of_appointment") or self.parse_date("1900-01-01"))

    def _is_role(self, director: dict, role: str) -> bool:
        designation = (director.get("designation") or "").strip().lower()
        if role == "auditor":
            return "auditor" in designation
        if role == "cfo":
            return designation == "cfo" or "chief financial officer" in designation
        return role in designation

    def _normalise_din(self, value) -> Optional[str]:
        raw = re.sub(r"\D", "", str(value or ""))
        if not raw:
            return None
        return raw.zfill(8)

    def _count_active_boards(self, din: Optional[str], current_cin: str) -> int:
        if not din:
            return 0
        row = self._fetchone(
            """
            SELECT COUNT(DISTINCT cin)
            FROM governance_graph
            WHERE din = %s
              AND (cessation_date IS NULL OR cessation_date > CURRENT_DATE)
            """,
            (din,),
        )
        count = (row[0] if row else 0) or 0
        current_present = self._fetchone(
            """
            SELECT 1
            FROM governance_graph
            WHERE din = %s AND cin = %s
              AND (cessation_date IS NULL OR cessation_date > CURRENT_DATE)
            LIMIT 1
            """,
            (din, current_cin),
        )
        if not current_present:
            count += 1
        return int(count)

    def _upsert_governance_graph(self, cin: str, director: dict) -> None:
        din = director.get("din")
        if not din:
            return
        appointment_date = director.get("date_of_appointment")
        try:
            self._execute(
                """
                INSERT INTO governance_graph (
                    din, cin, director_name, designation, date_of_appointment,
                    cessation_date, is_active, source
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (din, cin, date_of_appointment) DO UPDATE
                  SET director_name = EXCLUDED.director_name,
                      designation = EXCLUDED.designation,
                      cessation_date = EXCLUDED.cessation_date,
                      is_active = EXCLUDED.is_active,
                      source = EXCLUDED.source
                """,
                (
                    din,
                    cin,
                    director.get("director_name"),
                    director.get("designation"),
                    appointment_date,
                    director.get("cessation_date"),
                    director.get("is_active", True),
                    self.source_id,
                ),
            )
            self._commit()
        except Exception as exc:
            logger.warning("mca_directors: governance graph upsert failed for cin=%s din=%s: %s", cin, din, exc)

    def _pick_value(self, payload: dict, *names: str):
        wanted = {name.lower() for name in names}
        for key, value in payload.items():
            key_norm = str(key).lower().replace(" ", "").replace("-", "").replace("_", "")
            if key_norm in wanted:
                return value
        for key, value in payload.items():
            key_norm = str(key).lower().replace(" ", "").replace("-", "").replace("_", "")
            if any(name in key_norm for name in wanted):
                return value
        return None

    def classify_change(self, previous: Optional[dict], current: dict, board_count: int = 0) -> list[tuple[str, str]]:
        events: list[tuple[str, str]] = []
        designation = (current.get("designation") or "").lower()
        if previous and not previous.get("cessation_date") and current.get("cessation_date"):
            if designation == "cfo" or "chief financial officer" in designation:
                events.append(("CFO_RESIGNED", "ALERT"))
            else:
                events.append(("DIRECTOR_RESIGNED", "WATCH"))
        if previous and "auditor" in designation and previous.get("director_name") != current.get("director_name"):
            events.append(("AUDITOR_CHANGED", "ALERT"))
        if board_count > 10:
            events.append(("DIRECTOR_OVERLOADED", "WATCH"))
        return events
