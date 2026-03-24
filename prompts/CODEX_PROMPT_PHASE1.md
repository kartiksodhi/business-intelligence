# Codex Handoff — Phase 1 Build

## 1. Your role

You are implementing Phase 1 of a corporate signal intelligence system. Claude Code is the architect — all specs are written. Your job is to implement them exactly. Do not make architecture decisions. If a spec is ambiguous, implement the most conservative interpretation and flag it in a comment.

---

## 2. What you are building

Phase 1 backend only. No frontend — that is Gemini's domain. No Phase 2 sources yet.

The system watches Indian government data sources, detects company-level events, scores their health impact, and routes alerts to operators. Phase 1 covers:

- **OGD data load** — Ingest the MCA Open Government Data CSV (~18 lakh companies) into a local PostgreSQL `master_entities` table.
- **Entity resolution** — Resolve every data fragment to a canonical CIN before it enters any graph table. Unresolved records go to a queue, not main tables.
- **Diff engine** — Compare each ingested snapshot against last known state. Fire events only on delta. Never reprocess unchanged records.
- **Health scoring** — Compute a company health score on event only, never on schedule. Recompute only the affected CIN.
- **Director graph** — Model director-company relationships. Detect contagion paths.
- **Operator CLI** — FastAPI `/op/` endpoints for status, watchlist management, manual triggers, and diagnostics.
- **Alert routing** — Score event, match watchlists, batch multi-event alerts, push to Telegram (CRITICAL) and email digest (daily, 7am).
- **Telegram bot** — CRITICAL severity only. Immediate push.
- **Daily digest** — 7am email via Resend. Top 5 events, costs, accuracy, scraper health.

---

## 3. Project structure to create

Create this exact directory and file layout:

```
bi_engine/
├── ingestion/
│   ├── __init__.py
│   ├── entity_resolver.py        ← implement from ENTITY_RESOLUTION_SPEC.md
│   └── ogd_loader.py             ← load MCA OGD CSV to master_entities
├── detection/
│   ├── __init__.py
│   ├── diff_engine.py            ← implement from DIFF_ENGINE_SPEC.md
│   ├── detectors/
│   │   ├── __init__.py
│   │   ├── base.py
│   │   ├── ogd.py
│   │   ├── nclt.py
│   │   ├── drt.py
│   │   ├── sarfaesi.py
│   │   ├── ecourts.py
│   │   └── directors.py
│   ├── health_scorer.py          ← implement from HEALTH_SCORE_SPEC.md
│   ├── shell_detector.py         ← implement from HEALTH_SCORE_SPEC.md
│   └── sector_cluster.py         ← implement from HEALTH_SCORE_SPEC.md
├── routing/
│   ├── __init__.py
│   ├── watchlist_matcher.py      ← implement from ALERTS_SPEC.md
│   ├── batch_flusher.py          ← implement from ALERTS_SPEC.md
│   ├── summarizer.py             ← implement from ALERTS_SPEC.md
│   ├── telegram_deliverer.py     ← implement from ALERTS_SPEC.md
│   ├── daily_digest.py           ← implement from ALERTS_SPEC.md
│   └── scheduler.py              ← implement from ALERTS_SPEC.md
├── api/
│   ├── __init__.py
│   ├── main.py                   ← implement from OPERATOR_CLI_SPEC.md
│   ├── dependencies.py
│   ├── models.py
│   └── routers/
│       └── operator.py
├── scripts/
│   └── load_ogd.py              ← standalone OGD loader script (one-shot, CLI runnable)
├── tests/
│   ├── test_entity_resolver.py
│   ├── test_diff_engine.py
│   ├── test_health_scorer.py
│   ├── test_operator_api.py
│   └── test_alerts.py
├── migrations/
│   └── 001_initial_schema.sql   ← copy DDL verbatim from SCHEMA_SPEC.md
├── .env.example
├── requirements.txt
└── README.md
```

Do not create files outside this structure without a spec-backed reason.

---

## 4. Build order

Follow this sequence exactly. Do not skip steps or reorder.

1. Create the full directory structure and `requirements.txt` (all dependencies pinned).
2. Copy schema DDL verbatim from `specs/SCHEMA_SPEC.md` → `migrations/001_initial_schema.sql`.
3. Run the migration against local PostgreSQL using `DATABASE_URL` from `.env`.
4. Build `ingestion/entity_resolver.py` from `specs/ENTITY_RESOLUTION_SPEC.md`.
5. Build `scripts/load_ogd.py` and `ingestion/ogd_loader.py` — depends on `entity_resolver` and `master_entities` table being present.
6. Build `detection/detectors/base.py` first, then each source detector (`ogd.py`, `nclt.py`, `drt.py`, `sarfaesi.py`, `ecourts.py`, `directors.py`).
7. Build `detection/diff_engine.py` — depends on detectors being complete.
8. Build `detection/health_scorer.py`, `detection/shell_detector.py`, `detection/sector_cluster.py` from `specs/HEALTH_SCORE_SPEC.md`.
9. Build all `routing/` modules in this order: `watchlist_matcher.py` → `batch_flusher.py` → `summarizer.py` → `telegram_deliverer.py` → `daily_digest.py` → `scheduler.py`. Each depends on the previous. Use `specs/ALERTS_SPEC.md`.
10. Build all `api/` modules in this order: `dependencies.py` → `models.py` → `routers/operator.py` → `main.py`. Use `specs/OPERATOR_CLI_SPEC.md`.
11. Run `pytest tests/ -v`. All tests must pass with zero failures.
12. Fix any failures before marking the build complete.

---

## 5. Spec files to read

All spec files are in the `specs/` directory relative to the project root. Read each one before implementing the corresponding module.

| Spec file | Implements |
|---|---|
| `specs/SCHEMA_SPEC.md` | `migrations/001_initial_schema.sql` — use DDL verbatim |
| `specs/ENTITY_RESOLUTION_SPEC.md` | `ingestion/entity_resolver.py` |
| `specs/DIFF_ENGINE_SPEC.md` | `detection/` layer — diff engine and all detectors |
| `specs/HEALTH_SCORE_SPEC.md` | `detection/health_scorer.py`, `shell_detector.py`, `sector_cluster.py` |
| `specs/OPERATOR_CLI_SPEC.md` | `api/` layer — all FastAPI endpoints |
| `specs/ALERTS_SPEC.md` | `routing/` layer — all alert and delivery modules |

If a spec file references a table column that does not match the schema DDL in `SCHEMA_SPEC.md`, the schema DDL wins. Flag the discrepancy in a `# SPEC MISMATCH:` comment at the point of deviation.

---

## 6. Environment variables

Create `.env.example` with exactly these variables (no values, just keys and inline comments):

```env
# PostgreSQL connection string
DATABASE_URL=

# Absolute path to the MCA OGD CSV on this machine
OGD_CSV_PATH=

# Claude API key — used only for LLM entity resolution fallback (max 500 calls/month) and alert summaries
CLAUDE_API_KEY=

# Telegram bot token from @BotFather
TELEGRAM_BOT_TOKEN=

# Telegram chat ID for the operator account that receives CRITICAL alerts
TELEGRAM_OPERATOR_CHAT_ID=

# Resend API key for daily digest emails
RESEND_API_KEY=

# Operator email address for daily digest delivery
OPERATOR_EMAIL=

# CompData API key — optional, not used in Phase 1, reserved for Phase 2 enrichment
COMPDATA_API_KEY=
```

All secrets must be loaded from environment variables at runtime. Never hardcode any secret, key, token, or credential in source code.

---

## 7. Hard constraints

Never break any of these:

1. **No architecture decisions.** Implement specs exactly as written. If a choice must be made that is not covered by a spec, use the most conservative interpretation and add a `# ARCH DECISION NEEDED:` comment.
2. **No external API calls for base company data.** The MCA OGD CSV is the source of truth for master entity data. Do not call any external API to populate `master_entities`.
3. **All secrets from environment variables.** No hardcoded values anywhere in source or config files.
4. **No f-string SQL.** Use parameterized queries with positional placeholders (`$1`, `$2`, ...) for all database queries. This is non-negotiable.
5. **All tests must pass before reporting done.** Run `pytest tests/ -v` and confirm zero failures. Do not mark the build complete if any test is failing or skipped.
6. **Schema DDL wins over spec prose.** If any spec file describes a column or table that conflicts with `SCHEMA_SPEC.md`, implement what the DDL says and flag the conflict.
7. **Install Playwright at setup:** run `playwright install chromium` as part of the setup sequence. Playwright handles all government portal automation. Do not use Firecrawl — it is not installed and requires operator approval.
8. **Diff, not reprocess.** Store source state per ingestion run. On the next run, process only the delta. Never reprocess unchanged records.
9. **Health score on event only.** Recompute health score only when a new event is detected for that CIN. Never recompute on a schedule.
10. **AI summary at alert delivery only.** Do not pre-generate summaries. The Claude API summarizer runs at the moment an alert is dispatched, not before.
11. **Deduplicate before summarizing.** Batch all events for the same CIN within a flush window before calling the summarizer. One summary per CIN per batch.

---

## 8. One question to ask before starting

Before writing any code, ask the operator exactly this:

> "Do you have the MCA OGD CSV file downloaded? If yes, provide the full path to the file. If no, I will add a download script to `scripts/download_ogd.py` that fetches the latest dataset from data.gov.in."

Wait for the answer before proceeding. If the operator provides a path, set that as the default for `OGD_CSV_PATH` in `.env.example` instructions. If not, build the download script.

---

## 9. Deliverables checklist

At the end of the build, confirm each item explicitly before reporting done:

- [ ] `migrations/001_initial_schema.sql` exists and runs cleanly against a fresh PostgreSQL database with zero errors
- [ ] `scripts/load_ogd.py` runs to completion against the provided OGD CSV with no unhandled exceptions
- [ ] `pytest tests/ -v` reports 0 failures, 0 errors
- [ ] FastAPI server starts cleanly: `uvicorn api.main:app --reload`
- [ ] `GET /op/status` returns valid JSON with HTTP 200
- [ ] `.env.example` contains all required variables listed in Section 6

Do not report the build as complete until every item on this checklist is confirmed.
