#!/usr/bin/env python3
import asyncio
import csv
import os
import re
import string
import sys
from datetime import date, datetime
from pathlib import Path

import asyncpg
from dotenv import load_dotenv


CIN_PATTERN = re.compile(r"^[UL]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$")
DATE_FORMATS = (
    "%Y-%m-%d",
    "%d-%m-%Y",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d-%b-%Y",
    "%d-%B-%Y",
)
NORMALIZATION_PATTERN = re.compile(r"\b(?:pvt|ltd|private|limited|india|llp)\b")
HEADER_ALIASES = {
    "cin": (
        "cin",
        "company_cin",
        "corporate_identity_number",
        "company_identification_number",
    ),
    "company_name": ("company_name", "name", "company", "companyname"),
    "status": ("status", "company_status", "companystatus"),
    "registered_state": ("registered_state", "state", "state_code", "companystatecode"),
    "industrial_class": (
        "industrial_class",
        "nic_code",
        "industry_code",
        "industrial_activity",
        "companyindustrialclassification",
    ),
    "date_of_incorporation": (
        "date_of_incorporation",
        "incorporation_date",
        "date_of_registration",
        "companyregistrationdate_date",
    ),
    "date_of_last_agm": ("date_of_last_agm", "last_agm_date", "agm_date"),
    "authorized_capital": ("authorized_capital", "authorised_capital", "authorizedcapital"),
    "paid_up_capital": ("paid_up_capital", "paidup_capital", "paidupcapital"),
    "company_category": ("company_category", "category", "companycategory"),
    "company_subcategory": ("company_subcategory", "subcategory", "companysubcategory"),
    "registered_address": (
        "registered_address",
        "address",
        "registered_office_address",
        "registered_office_address",
    ),
    "email": ("email", "email_id", "company_email"),
    "pan": ("pan", "company_pan"),
}
PUNCT_TRANSLATION = str.maketrans("", "", string.punctuation)
COPY_COLUMNS = [
    "cin", "company_name", "normalized_name", "status", "registered_state",
    "industrial_class", "date_of_incorporation", "date_of_last_agm",
    "authorized_capital", "paid_up_capital", "company_category",
    "company_subcategory", "registered_address", "email", "pan",
]
COPY_BATCH_SIZE = 50_000


def fail(message: str) -> None:
    print(message, file=sys.stderr)
    raise SystemExit(1)


def canonicalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def resolve_headers(fieldnames: list[str]) -> dict[str, str]:
    normalized_headers = {canonicalize_header(name): name for name in fieldnames}
    resolved: dict[str, str] = {}

    for target, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            if alias in normalized_headers:
                resolved[target] = normalized_headers[alias]
                break

    for required in ("cin", "company_name", "status"):
        if required not in resolved:
            fail(f"Missing required CSV column for '{required}'.")

    return resolved


def get_value(row: dict[str, str], header_map: dict[str, str], field: str) -> str:
    header = header_map.get(field)
    if header is None:
        return ""
    return (row.get(header) or "").strip()


def parse_optional_date(value: str) -> date | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    for date_format in DATE_FORMATS:
        try:
            return datetime.strptime(cleaned, date_format).date()
        except ValueError:
            continue

    fail_reason = f"unparseable date '{value}'"
    raise ValueError(fail_reason)


def parse_optional_int(value: str) -> int | None:
    cleaned = value.strip()
    if not cleaned:
        return None

    normalized = cleaned.replace(",", "")
    if "." in normalized:
        normalized = normalized.split(".", 1)[0]  # truncate decimals

    try:
        return int(normalized)
    except ValueError as exc:
        raise ValueError(f"invalid integer '{value}'") from exc


def normalize_company_name(company_name: str) -> str:
    lowered = company_name.lower()
    stripped_terms = NORMALIZATION_PATTERN.sub(" ", lowered)
    no_punctuation = stripped_terms.translate(PUNCT_TRANSLATION)
    collapsed = re.sub(r"\s+", " ", no_punctuation).strip()
    return collapsed


def validate_row(
    row: dict[str, str], header_map: dict[str, str]
) -> tuple[dict[str, object], list[str]]:
    reasons: list[str] = []
    cin = get_value(row, header_map, "cin").upper()
    company_name = get_value(row, header_map, "company_name")
    status = get_value(row, header_map, "status")

    if not CIN_PATTERN.fullmatch(cin):
        reasons.append("invalid CIN format")

    if not company_name:
        reasons.append("empty company name")

    if not status:
        reasons.append("empty status")

    try:
        date_of_incorporation = parse_optional_date(
            get_value(row, header_map, "date_of_incorporation")
        )
    except ValueError as exc:
        reasons.append(str(exc))
        date_of_incorporation = None

    try:
        date_of_last_agm = parse_optional_date(
            get_value(row, header_map, "date_of_last_agm")
        )
    except ValueError as exc:
        reasons.append(str(exc))
        date_of_last_agm = None

    try:
        authorized_capital = parse_optional_int(
            get_value(row, header_map, "authorized_capital")
        )
    except ValueError as exc:
        reasons.append(str(exc))
        authorized_capital = None

    try:
        paid_up_capital = parse_optional_int(get_value(row, header_map, "paid_up_capital"))
    except ValueError as exc:
        reasons.append(str(exc))
        paid_up_capital = None

    payload = {
        "cin": cin,
        "company_name": company_name,
        "normalized_name": normalize_company_name(company_name) if company_name else "",
        "status": status,
        "registered_state": get_value(row, header_map, "registered_state") or None,
        "industrial_class": get_value(row, header_map, "industrial_class") or None,
        "date_of_incorporation": date_of_incorporation,
        "date_of_last_agm": date_of_last_agm,
        "authorized_capital": authorized_capital,
        "paid_up_capital": paid_up_capital,
        "company_category": get_value(row, header_map, "company_category") or None,
        "company_subcategory": get_value(row, header_map, "company_subcategory") or None,
        "registered_address": get_value(row, header_map, "registered_address") or None,
        "email": get_value(row, header_map, "email") or None,
        "pan": get_value(row, header_map, "pan") or None,
    }
    return payload, reasons


async def flush_batch(conn: asyncpg.Connection, batch: list[tuple]) -> None:
    await conn.copy_records_to_table("ogd_staging", records=batch, columns=COPY_COLUMNS)
    await conn.execute("""
        INSERT INTO master_entities (
            cin, company_name, normalized_name, status, registered_state,
            industrial_class, date_of_incorporation, date_of_last_agm,
            authorized_capital, paid_up_capital, company_category,
            company_subcategory, registered_address, email, pan
        )
        SELECT DISTINCT ON (cin) cin, company_name, normalized_name, status, registered_state,
               industrial_class, date_of_incorporation, date_of_last_agm,
               authorized_capital, paid_up_capital, company_category,
               company_subcategory, registered_address, email, pan
        FROM ogd_staging
        ORDER BY cin
        ON CONFLICT (cin) DO UPDATE SET
            company_name = EXCLUDED.company_name,
            normalized_name = EXCLUDED.normalized_name,
            status = EXCLUDED.status,
            registered_state = EXCLUDED.registered_state,
            industrial_class = EXCLUDED.industrial_class,
            date_of_incorporation = EXCLUDED.date_of_incorporation,
            date_of_last_agm = EXCLUDED.date_of_last_agm,
            authorized_capital = EXCLUDED.authorized_capital,
            paid_up_capital = EXCLUDED.paid_up_capital,
            company_category = EXCLUDED.company_category,
            company_subcategory = EXCLUDED.company_subcategory,
            registered_address = EXCLUDED.registered_address,
            email = EXCLUDED.email,
            pan = EXCLUDED.pan
    """)
    await conn.execute("TRUNCATE ogd_staging")


async def load_rows(database_url: str, csv_path: Path) -> None:
    total_rows = 0
    skipped = 0

    conn = await asyncpg.connect(database_url)
    try:
        await conn.execute("""
            CREATE TEMP TABLE ogd_staging (
                cin text,
                company_name text,
                normalized_name text,
                status text,
                registered_state text,
                industrial_class text,
                date_of_incorporation date,
                date_of_last_agm date,
                authorized_capital bigint,
                paid_up_capital bigint,
                company_category text,
                company_subcategory text,
                registered_address text,
                email text,
                pan text
            )
        """)

        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            if not reader.fieldnames:
                fail("CSV file has no header row.")

            header_map = resolve_headers(reader.fieldnames)
            batch: list[tuple] = []

            for row_number, row in enumerate(reader, start=2):
                total_rows += 1
                payload, reasons = validate_row(row, header_map)
                if reasons:
                    skipped += 1
                    continue

                batch.append((
                    payload["cin"],
                    payload["company_name"],
                    payload["normalized_name"],
                    payload["status"],
                    payload["registered_state"],
                    payload["industrial_class"],
                    payload["date_of_incorporation"],
                    payload["date_of_last_agm"],
                    payload["authorized_capital"],
                    payload["paid_up_capital"],
                    payload["company_category"],
                    payload["company_subcategory"],
                    payload["registered_address"],
                    payload["email"],
                    payload["pan"],
                ))

                if len(batch) >= COPY_BATCH_SIZE:
                    await flush_batch(conn, batch)
                    loaded = total_rows - skipped
                    print(f"  {loaded:,} rows upserted so far...", flush=True)
                    batch = []

            if batch:
                await flush_batch(conn, batch)

    finally:
        await conn.close()

    loaded = total_rows - skipped
    print(f"Total rows in CSV: {total_rows:,}")
    print(f"Upserted: {loaded:,}")
    print(f"Skipped (invalid): {skipped:,}")


async def main() -> None:
    load_dotenv()

    database_url = os.getenv("DATABASE_URL", "").strip()
    if not database_url:
        fail("DATABASE_URL is required.")

    csv_path_value = os.getenv("OGD_CSV_PATH", "").strip()
    if not csv_path_value:
        fail("OGD_CSV_PATH is required.")

    csv_path = Path(csv_path_value)
    if not csv_path.is_file():
        fail(f"OGD CSV not found at '{csv_path}'.")

    try:
        await load_rows(database_url, csv_path)
    except asyncpg.UndefinedTableError:
        fail(
            "master_entities does not exist. Apply the schema DDL from Claude Code "
            "before running this loader."
        )
    except asyncpg.PostgresError as exc:
        fail(f"Database error: {exc}")


if __name__ == "__main__":
    asyncio.run(main())
