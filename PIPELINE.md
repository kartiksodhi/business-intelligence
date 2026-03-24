# PIPELINE.md
## Read this only when working on ingestion, detection, routing, alerts, or operator interface.

---

## Step 1: Ingestion

**CAPTCHA strategy (priority order):**
1. Check for API/download endpoint first — many portals have them
2. Playwright + pytesseract OCR — free, ~60% success on government CAPTCHAs
3. 2captcha fallback — ₹500/month budget, every solve logged in `captcha_log`
4. Manual queue — operator solves via `POST /op/captcha/solve`, system resumes automatically

**Source failure:** Retry: 5min → 15min → 1hr → 4hr. After 4 failures → SOURCE_UNREACHABLE operator alert. Mark degraded in `source_state`. Other sources unaffected.

**Schema validation:** After parsing, check expected fields present and values in range. Fail = log SCRAPER_BROKEN, skip record, alert operator. Never silently ingest bad data.

---

## Step 2: Validation rules
- CIN: 21 chars, U/L + 5digits + state + year + PVT/PLC + 6digits
- Dates: parseable, not future, not before 1956
- Capital: numeric, positive, >₹10,000Cr = manual review flag
- Names: not empty, minimum 3 chars
- Invalid records → `validation_queue` with reason. Never to main tables.

---

## Step 3: Storage — identifier mapping

EPFO and GSTIN are not in OGD. Build mapping passively:
1. Parse AOC-4 filings — extract EPFO code and GSTIN, store in `identifier_map`
2. CompData enrichment returns both — cache immediately
3. Signal without mapping → `unmapped_signals` (not discarded)
4. Weekly job retroactively processes `unmapped_signals` as mapping builds

```sql
CREATE TABLE identifier_map (
  cin VARCHAR(21), identifier_type VARCHAR(20),
  identifier_value VARCHAR(50), source VARCHAR(50),
  PRIMARY KEY (identifier_type, identifier_value)
);
CREATE TABLE unmapped_signals (
  id SERIAL PRIMARY KEY, source VARCHAR(50),
  identifier_type VARCHAR(20), identifier_value VARCHAR(50),
  raw_data JSONB, detected_at TIMESTAMP, resolved BOOLEAN DEFAULT FALSE
);
```

---

## Step 4: Entity resolution pipeline

1. Exact CIN/PAN match — 100%
2. Exact normalized name (strip pvt/ltd/private/limited) — 95%
3. pg_trgm trigram > 0.8 + same state — 90%
4. Jaro-Winkler > 0.85 + same state — 85%
5. Trigram > 0.6 + state + industry — 75%
6. Trigram > 0.6 alone — 60%
7. Below 60% → `entity_resolution_queue`
8. Queue at 50-70%, 2-3 candidates → Claude API LLM fallback (max 500/month)
9. Below 50% after LLM → UNRESOLVABLE, operator weekly digest

---

## Step 5: Detection

`source_state` stores last_pull_timestamp and last_data_hash per source.
Hash unchanged → nothing fires, zero cost.
Meaningful change thresholds defined per source in SOURCES.md.

---

## Step 6: Scoring — health score components

| Component | Weight | Scoring |
|---|---|---|
| Filing freshness | 25% | <12mo=100, 12-18=70, 18-24=40, >36=0 |
| Director stability | 20% | 0 changes=100, 1=80, 2=50, 3+=20 |
| Legal risk | 25% | 0 cases=100, NCLT filing=5 |
| Financial health | 20% | D/E<1=100, >4=15, no data=50 |
| Capital trajectory | 10% | Increased=100, stable=60, decreased=20 |

Contagion penalty: -15 per RED director connection, -5 per AMBER.
Recomputes on event only. Never on schedule.

---

## Step 7: Contagion

Company crosses AMBER/RED:
1. Get all active DINs from governance_graph
2. Find all other CINs those DINs direct
3. Apply contagion penalty, recompute their scores
4. Max depth 2. Log full chain.

**Shell company detection** — fires when all true simultaneously:
Incorporated under 36 months + capital under ₹10L + no AGM + zero EPFO + zero GSTIN + directors on 5+ other boards → SHELL_RISK (WATCH severity)

**Sector cluster detection** — monthly after OGD diff:
5+ companies same state + NIC code → AMBER/RED within 30 days → SECTOR_CLUSTER_ALERT → sector watchlists only

---

## Step 8: Routing

Watchlist filters: CIN list, state, sector (NIC), severity minimum. All filters must match.
Same event → multiple watchlists → independent alerts to each. Correct behaviour.
New subscriber backfill: off by default. Optional paid: 90-day, labeled historical.

---

## Step 9: Deduplication

Batch windows by severity:
- CRITICAL: 0 seconds (immediate)
- ALERT: 4 hours
- WATCH: 24 hours
- INFO: 7 days

Multiple events same CIN within window → one digest alert. Batch flush every 30 minutes. One Claude API call per flush, not per event.

---

## Step 10: Alert format — every alert must contain

1. Company name + CIN + sector + state
2. Event type + source + detected timestamp
3. Health score: current → previous with direction
4. All contributing signals (not just trigger)
5. Contagion result
6. Data latency label per source
7. Confidence: HIGH/MEDIUM/LOW
8. AI summary: 3 sentences, plain English, one Claude API call, generated at delivery only
9. Suggested watchlist additions from contagion

---

## Step 11: Operator delivery

**Dashboard screens (Gemini builds):**
- Live feed: events 24 hours, filter by severity/source/sector/state
- Company profile: search by name/CIN, event history, score timeline
- Source monitor: every scraper status, last pull, failure flags
- Watchlist manager: create/edit/delete with all filter dimensions
- Accuracy tracker: RED alerts fired vs confirmed, false positive log
- Cost monitor: daily tokens, API calls, running monthly total

**Operator CLI endpoints:**
```
GET  /op/status           → all sources: last pull, record count, failures
GET  /op/events/today     → everything fired last 24 hours
GET  /op/health/{cin}     → spot check any company
GET  /op/sources/lag      → sources behind cadence
GET  /op/accuracy         → rolling prediction accuracy rate
GET  /op/costs/today      → tokens, API calls, cost today
POST /op/watchlist        → add watchlist without UI
POST /op/enrich/{cin}     → manually trigger enrichment
POST /op/resolve          → manually resolve entity resolution queue item
POST /op/recalibrate      → trigger monthly weight review
POST /op/captcha/solve    → submit manual CAPTCHA solve
```

**Telegram CRITICAL push:**
```python
message = f"CRITICAL: {company.name}\nEvent: {event.event_type}\nScore: {prev}→{current}\nCIN: {cin}"
await bot.send_message(chat_id=OPERATOR_CHAT_ID, text=message)
```

---

## Step 12: Feedback loop

```sql
CREATE TABLE predictions (
  cin VARCHAR(21), event_combination TEXT[],
  health_score_at_firing INT, severity VARCHAR(20),
  fired_at TIMESTAMP, confirmed BOOLEAN,
  confirmed_at TIMESTAMP, false_positive BOOLEAN,
  auto_confirmed BOOLEAN DEFAULT FALSE
);
```

**Auto-confirm daily:** NCLT admission + fired_at within 180 days → confirm. SARFAESI auction → confirm. Struck Off status → confirm. 180 days no confirmation → expire.

**Monthly recalibration (1st of month):**
Per signal component: confirmed_rate vs current weight → adjust ±0.05, bounded 0.1→1.0.
Claude Code reviews all changes before applying (Phase 1-3).
Phase 4: automated with approval gate for changes over 0.2.

**Accuracy report in daily digest:**
"Last 30 days: X RED alerts. Y confirmed (Z%). Top false positive cause: [reason]."
