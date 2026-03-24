# SKILLS.md

## Entity resolution

The moat. Every external data source uses messy names. The Golden Record uses CINs. Bridging them is everything.

**Pipeline (cheapest first):**
1. Exact CIN/PAN match if present in source — 100% confidence
2. Exact normalized name (lowercase, strip pvt/ltd/private/limited/india, remove punctuation) — 95%
3. pg_trgm trigram similarity > 0.8 + same state — 90%
4. Jaro-Winkler > 0.85 + same state — 85%
5. Trigram > 0.6 + same state + same industry — 75%
6. Trigram > 0.6 alone — 60%
7. Below 60% → `entity_resolution_queue`
8. Queue items at 50-70% with 2-3 candidates → LLM fallback (max 500/month)

**Required indexes:**
```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX idx_name_trgm ON master_entities USING gin (company_name gin_trgm_ops);
CREATE INDEX idx_cin ON master_entities (cin);
CREATE INDEX idx_state ON master_entities (registered_state);
CREATE INDEX idx_nic ON master_entities (industrial_class);
CREATE INDEX idx_status ON master_entities (status);
```

## Event detection — the diff engine

Every source has a `source_state` row storing last pull hash and timestamp.

**Diff pattern (universal across all sources):**
```
1. Pull new data from source
2. Load last known state from source_state
3. Compare: what's new, what changed, what disappeared
4. For each change → create event record in events table
5. Update source_state with new hash and timestamp
6. If nothing changed → log, do nothing
```

**Event record structure:**
```sql
INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
```

Severity levels: INFO (logged only), WATCH (tracked), ALERT (pushed), CRITICAL (immediate push + contagion check).

## Health scoring

Composite score 0-100. Computed from local data only. Recomputed when underlying event fires, not on schedule.

**Components:**

| Component | Weight | Source | Scoring |
|---|---|---|---|
| Filing freshness | 25% | OGD AGM date | <12mo=100, 12-18=70, 18-24=40, 24-36=15, >36=0 |
| Director stability | 20% | governance_graph | 0 changes=100, 1=80, 2=50, 3+=20 |
| Legal risk | 25% | legal_events | 0 cases=100, 1 non-138=80, 1-2 Sec138=50, 3+=20, NCLT=5 |
| Financial health | 20% | financial_snapshots | D/E<1=100, 1-2=70, 2-4=40, >4=15 (neutral 50 if no data) |
| Capital trajectory | 10% | OGD diffs | Increased=100, stable=60, decreased=20 |

**Contagion penalty:** For each director shared with a RED company: -15 points. AMBER: -5 points.

**Score triggers recompute, not schedule.** New legal event on CIN → recompute that CIN's score. Director resigned → recompute. Don't batch-recompute all 18L nightly.

## Contagion propagation

When a company crosses into AMBER or RED:
```
1. Look up all active directors (DINs) from governance_graph
2. For each DIN, find all OTHER companies they direct
3. Apply contagion penalty to those companies
4. If any of those cross a threshold → fire event → recurse (max depth 2)
5. Log the full contagion chain
```

This is how "Promoter X's main company is in trouble" automatically surfaces risk across his entire group.

## Ingestion patterns

**Bulk file sources (OGD, SEBI bulk deals, RBI lists):**
Download file → parse → load to staging → diff against master → fire events → update master.

**Web scrape sources (e-Courts, NCLT, DRT, SARFAESI, GeM):**
Scrape by date/recency → parse → entity resolve respondent/company names → match to CINs → fire events.

**API sources (CompData, GST portal):**
Call only when triggered → cache response → update enrichment_cache → fire events from any changes detected.

**Firecrawl sources (Naukri, Glassdoor, career pages):**
Crawl targeted company pages → extract structured data (job count, review count/rating) → compare to last crawl → fire events on significant changes.

## Watchlist matching

```sql
-- When event fires, check all watchlists
SELECT w.id, w.filters
FROM watchlists w
WHERE
  (w.cin_list IS NULL OR event.cin = ANY(w.cin_list))
  AND (w.state_filter IS NULL OR event.company_state = w.state_filter)
  AND (w.sector_filter IS NULL OR event.company_nic = w.sector_filter)
  AND (w.severity_min IS NULL OR event.severity >= w.severity_min)
  AND (w.signal_types IS NULL OR event.event_type = ANY(w.signal_types));
```

Matching events get inserted into `alerts` table and pushed.

## Search optimization (internal, not user-facing)

The system searches its own data constantly — entity resolution, contagion lookups, watchlist matching. Every query must be fast.

```sql
-- Decay scan (monthly after OGD load)
CREATE INDEX idx_decay ON master_entities (status, date_of_last_agm) WHERE status = 'Active';

-- Director contagion lookup
CREATE INDEX idx_gov_din ON governance_graph (din) WHERE cessation_date IS NULL;
CREATE INDEX idx_gov_cin ON governance_graph (cin);

-- Event stream queries
CREATE INDEX idx_events_cin ON events (cin, detected_at DESC);
CREATE INDEX idx_events_type ON events (event_type, detected_at DESC);
CREATE INDEX idx_events_severity ON events (severity, detected_at DESC);

-- Watchlist matching
CREATE INDEX idx_alerts_watchlist ON alerts (watchlist_id, delivered_at DESC);

-- Cache expiry
CREATE INDEX idx_cache_expiry ON enrichment_cache (cin, source, expires_at);
```

## Alert formatting

When an event matches a watchlist and crosses threshold:

```
[ALERT — CRITICAL] 15 Mar 2026, 11:01 PM
Company: Gujarat Positra Steel Pvt Ltd (U27100GJ2015PTC082456)
Event: 3rd Section 138 cheque bounce case filed (e-Courts Ahmedabad)
Health score: 28 (RED) ↓ from 52 last month
Contributing signals:
  - Filing decay: last AGM 22 months ago
  - Legal: 3 active cheque bounce cases
  - Director: 1 resignation in last 90 days
Contagion: Director DIN 07654321 also sits on 2 other boards
  - ABC Industries (CIN ...) → score 65, now under watch
  - XYZ Trading (CIN ...) → score 71, no change
```

AI summary generated only at this point. One Claude API call per alert delivery. Not pre-generated.
