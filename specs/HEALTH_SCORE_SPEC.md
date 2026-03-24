# HEALTH_SCORE_SPEC.md

## Read this before implementing health scoring, contagion, shell detection, or sector cluster detection.

This spec is the authoritative implementation guide for Codex. It defines every scoring rule, data query, propagation boundary, persistence step, and test case. Do not deviate from the rules here without operator approval.

---

## What this layer does

When an event fires, the health scorer reads local DB state for the affected CIN and recomputes a composite 0–100 score. No external API calls. No scheduled recomputes. Score changes propagate through the director graph to connected companies (contagion). Two secondary detectors — shell risk and sector cluster — fire independent events when structural anomalies are found.

---

## Files to produce

```
detection/health_scorer.py       — HealthScorer, ScoreResult, ContagionPropagator
detection/shell_detector.py      — ShellDetector
detection/sector_cluster.py      — SectorClusterDetector
tests/test_health_scorer.py      — 15 pytest cases (all with mocked DB)
```

---

## Database tables referenced

All reads are from local PostgreSQL. Schema is defined in `SCHEMA_SPEC.md`. The tables used by this layer:

| Table | Used for |
|---|---|
| `master_entities` | AGM date, paid_up_capital, incorporation date, status, registered_state, industrial_class, health_score, health_band |
| `governance_graph` | Active directors per CIN, all CINs per DIN |
| `legal_events` | Active case counts by type |
| `financial_snapshots` | Latest year D/E ratio |
| `events` | Writing CONTAGION_PROPAGATED, SHELL_RISK, SECTOR_CLUSTER_ALERT; reading recent cluster events |
| `identifier_map` | EPFO and GSTIN presence check (shell detection) |
| `predictions` | Writing score-at-firing for feedback loop |

---

## Health score formula

### Overview

Five components with fixed weights. Weights sum to exactly 1.0. Raw score is the weighted sum. A contagion penalty is applied after. Final score is clamped to [0, 100].

```
raw_score = sum(component_score_i * weight_i)  for i in {1..5}
final_score = max(0, min(100, raw_score - contagion_penalty))
```

### Component table

| Component | Weight | Source table + column | Scoring logic |
|---|---|---|---|
| `filing_freshness` | 0.25 | `master_entities.date_of_last_agm` | See rules below |
| `director_stability` | 0.20 | `governance_graph` | Count director changes in last 90 days |
| `legal_risk` | 0.25 | `legal_events` (active cases only) | See rules below |
| `financial_health` | 0.20 | `financial_snapshots` (latest year, by max financial_year) | D/E ratio bands |
| `capital_trajectory` | 0.10 | `master_entities` vs previous OGD snapshot in `events.data_json` | Capital delta % |

### Component scoring rules

**filing_freshness** — compute months since `date_of_last_agm` as of NOW():

| Condition | Score |
|---|---|
| AGM within last 12 months | 100 |
| 12–18 months ago | 70 |
| 18–24 months ago | 40 |
| 24–36 months ago | 15 |
| More than 36 months ago | 0 |
| NULL and company status = 'Active' | 30 |
| NULL and company status != 'Active' | 0 |

Month calculation: use `relativedelta` or `(NOW() - date_of_last_agm).days / 30.44`. Do not use calendar month arithmetic that rounds incorrectly.

---

**director_stability** — count rows in `governance_graph` for this CIN where:
- `date_of_appointment >= NOW() - INTERVAL '90 days'` (appointments)
- OR `cessation_date >= NOW() - INTERVAL '90 days'` (cessations)

Total director change events (appointments + cessations) in that 90-day window:

| Change count | Score |
|---|---|
| 0 | 100 |
| 1 | 80 |
| 2 | 50 |
| 3 or more | 20 |

---

**legal_risk** — query `legal_events` for this CIN where `status` is NOT one of: `'Disposed'`, `'Dismissed'`, `'Withdrawn'`, `'Closed'`. These are active cases.

Evaluation order is strict — apply the FIRST matching rule:

| Condition | Score |
|---|---|
| Any row where `case_type IN ('SARFAESI_AUCTION', 'SARFAESI_13_4')` | 5 |
| Any row where `case_type IN ('NCLT_7', 'NCLT_9', 'NCLT_10')` | 5 |
| 3 or more active cases of any type | 20 |
| 1 or 2 active cases where `case_type = 'SEC_138'` | 50 |
| Exactly 1 active case where `case_type != 'SEC_138'` | 80 |
| 0 active cases | 100 |

Evaluation order matters. SARFAESI/NCLT rules take precedence regardless of case count.

---

**financial_health** — select the row from `financial_snapshots` for this CIN with the highest `financial_year` value (lexicographic sort on `'2023-24'` format is correct for these strings). Read `debt_to_equity`:

| D/E ratio | Score |
|---|---|
| < 1.0 | 100 |
| 1.0 – 2.0 (inclusive) | 70 |
| > 2.0 – 4.0 (inclusive) | 40 |
| > 4.0 | 15 |
| No row found for this CIN | 50 (neutral) |
| Row exists but `debt_to_equity` IS NULL | 50 (neutral) |

---

**capital_trajectory** — compare `master_entities.paid_up_capital` (current value) to the previous OGD snapshot.

Previous snapshot retrieval: query `events` table for this CIN where `event_type = 'CAPITAL_CHANGED'` or `event_type = 'OGD_UPDATED'`, ordered by `detected_at DESC`, limit 1. Read `data_json->>'previous_paid_up_capital'` as the prior value. If no such event exists, the previous value is unknown.

| Condition | Score |
|---|---|
| Capital increased by more than 5% relative to previous | 100 |
| Capital stable (within ±5% of previous) | 60 |
| Capital decreased by more than 5% relative to previous | 20 |
| No prior snapshot found | 50 (neutral) |
| `paid_up_capital` IS NULL | 50 (neutral) |

Percent change formula: `((current - previous) / previous) * 100`. Both values must be non-zero for the calculation to proceed; otherwise return 50.

---

### Contagion penalty

Applied after raw score is computed. Rules:

1. Query `governance_graph` for all rows where `cin = $cin` AND `is_active = TRUE`. Collect the set of DINs.
2. For each DIN in that set: query `governance_graph` for all OTHER CINs where `din = $din` AND `is_active = TRUE` AND `cin != $cin`. This produces the set of "connected CINs" via shared directors.
3. For each connected CIN: read `master_entities.health_band`.
4. Accumulate penalties:
   - Each connected CIN with `health_band = 'RED'`: add 15 to penalty
   - Each connected CIN with `health_band = 'AMBER'`: add 5 to penalty
   - Other bands contribute 0
5. A single connected CIN is counted once even if multiple directors connect to it (deduplicate connected CINs before accumulating).
6. `final_score = max(0, min(100, raw_score - contagion_penalty))`

The floor of 0 is absolute. Contagion cannot produce a negative score.

---

### Health bands

| Band | Score range |
|---|---|
| GREEN | 70 – 100 (inclusive) |
| AMBER | 40 – 69 (inclusive) |
| RED | 0 – 39 (inclusive) |
| WATCH | Overlay flag — not a score range. See shell detection and sector cluster sections. |

`health_band` stored in `master_entities` is set to `'WATCH'` only when a `SHELL_RISK` or `SECTOR_CLUSTER_ALERT` event is active for this CIN. This is an overlay: a company with score 75 is GREEN but may also carry a WATCH flag. The band stored in `master_entities.health_band` should reflect the score-derived band (GREEN/AMBER/RED) unless operator logic applies the WATCH overlay explicitly. `ScoreResult.band` always returns the score-derived band, never WATCH.

---

## Python implementation

### ScoreResult dataclass

```python
# detection/health_scorer.py

from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Dict, Any


@dataclass
class ScoreResult:
    cin: str
    score: float                    # 0–100, rounded to 1 decimal place
    band: str                       # 'GREEN' | 'AMBER' | 'RED'
    previous_score: float           # score before this recompute; 50.0 if no prior record
    previous_band: str              # band before this recompute; 'AMBER' if no prior record
    components: Dict[str, Dict[str, float]]
    # {
    #   'filing_freshness':    {'raw': 70.0, 'weight': 0.25, 'weighted': 17.5},
    #   'director_stability':  {'raw': 100.0, 'weight': 0.20, 'weighted': 20.0},
    #   'legal_risk':          {'raw': 100.0, 'weight': 0.25, 'weighted': 25.0},
    #   'financial_health':    {'raw': 50.0,  'weight': 0.20, 'weighted': 10.0},
    #   'capital_trajectory':  {'raw': 60.0,  'weight': 0.10, 'weighted': 6.0},
    # }
    contagion_penalty: float        # total penalty subtracted; 0.0 if none
    contagion_sources: List[str]    # CINs that contributed to the penalty
    triggering_event_id: int
    computed_at: datetime
```

No optional fields. All fields always populated. `previous_score` defaults to `50.0` and `previous_band` defaults to `'AMBER'` when no prior `health_score` exists in `master_entities`.

---

### HealthScorer class

```python
class HealthScorer:
    def __init__(self, db_pool):
        # db_pool: asyncpg connection pool
        self.db = db_pool

    async def recompute(self, cin: str, triggering_event_id: int) -> ScoreResult:
        ...

    async def _compute_filing_freshness(self, cin: str) -> float:
        ...

    async def _compute_director_stability(self, cin: str) -> float:
        ...

    async def _compute_legal_risk(self, cin: str) -> float:
        ...

    async def _compute_financial_health(self, cin: str) -> float:
        ...

    async def _compute_capital_trajectory(self, cin: str) -> float:
        ...

    async def _apply_contagion_penalty(
        self, cin: str, raw_score: float
    ) -> tuple[float, float, list[str]]:
        # returns (final_score, penalty_applied, contagion_source_cins)
        ...

    async def _get_band(self, score: float) -> str:
        ...

    async def _persist_score(
        self,
        cin: str,
        score: float,
        band: str,
        components: dict,
        event_id: int,
        previous_score: float,
        previous_band: str,
    ) -> None:
        ...
```

#### recompute — step-by-step logic

```
1.  Read master_entities row for this cin — get current health_score as previous_score,
    health_band as previous_band. If health_score IS NULL, use 50.0 / 'AMBER'.

2.  Call all five _compute_* methods concurrently (asyncio.gather).

3.  Compute raw_score:
    raw_score = (
        filing_freshness    * 0.25 +
        director_stability  * 0.20 +
        legal_risk          * 0.25 +
        financial_health    * 0.20 +
        capital_trajectory  * 0.10
    )

4.  Call _apply_contagion_penalty(cin, raw_score).
    Returns (final_score, penalty, contagion_sources).

5.  Round final_score to 1 decimal place.

6.  Call _get_band(final_score) to get band string.

7.  Build components dict with raw/weight/weighted for each component.

8.  Call _persist_score(...).

9.  Build and return ScoreResult.
```

#### _compute_filing_freshness — SQL

```sql
SELECT date_of_last_agm, status
FROM master_entities
WHERE cin = $1;
```

Apply scoring table from spec. Use Python `datetime.date.today()` as reference. Compute months as `(today - date_of_last_agm).days / 30.44`.

#### _compute_director_stability — SQL

```sql
SELECT COUNT(*) AS change_count
FROM governance_graph
WHERE cin = $1
  AND (
    date_of_appointment >= NOW() - INTERVAL '90 days'
    OR cessation_date >= NOW() - INTERVAL '90 days'
  );
```

Apply scoring table. `change_count` is the integer result of this query.

#### _compute_legal_risk — SQL

```sql
SELECT case_type, COUNT(*) AS cnt
FROM legal_events
WHERE cin = $1
  AND status NOT IN ('Disposed', 'Dismissed', 'Withdrawn', 'Closed')
GROUP BY case_type;
```

Evaluate in the exact precedence order defined in the scoring rules table. Build a dict `{case_type: count}` from the result. Check SARFAESI/NCLT first, then total count, then SEC_138 count, then single non-138 case, then zero.

#### _compute_financial_health — SQL

```sql
SELECT debt_to_equity
FROM financial_snapshots
WHERE cin = $1
ORDER BY financial_year DESC
LIMIT 1;
```

Apply D/E scoring table. Treat no-row and NULL `debt_to_equity` as 50.

#### _compute_capital_trajectory — SQL

```sql
SELECT data_json->>'previous_paid_up_capital' AS prev_capital
FROM events
WHERE cin = $1
  AND event_type IN ('CAPITAL_CHANGED', 'OGD_UPDATED')
  AND data_json ? 'previous_paid_up_capital'
ORDER BY detected_at DESC
LIMIT 1;
```

Also read `master_entities.paid_up_capital` for current value (can be fetched together with `filing_freshness` in a single query — optimization Codex may choose). Apply trajectory scoring.

#### _apply_contagion_penalty — SQL

Two queries:

```sql
-- Step 1: get active DINs for this CIN
SELECT din
FROM governance_graph
WHERE cin = $1
  AND is_active = TRUE;

-- Step 2: for each DIN, get all other CINs (batched with ANY)
SELECT DISTINCT gg.cin, me.health_band
FROM governance_graph gg
JOIN master_entities me ON me.cin = gg.cin
WHERE gg.din = ANY($1::varchar[])
  AND gg.is_active = TRUE
  AND gg.cin != $2;
```

Deduplicate connected CINs (a CIN appears once even if reached via multiple DINs). Accumulate penalty as defined. Return `(final_score, penalty, list_of_contributing_cins)`.

#### _get_band — logic

```python
async def _get_band(self, score: float) -> str:
    if score >= 70:
        return 'GREEN'
    elif score >= 40:
        return 'AMBER'
    else:
        return 'RED'
```

#### _persist_score — three writes, all in a single DB transaction

```sql
-- 1. Update master_entities
UPDATE master_entities
SET health_score = $1,
    health_band = $2,
    last_score_computed_at = NOW()
WHERE cin = $3;

-- 2. Update the triggering event row
UPDATE events
SET health_score_before = $1,
    health_score_after = $2
WHERE id = $3;

-- 3. Insert into predictions for feedback loop
INSERT INTO predictions (cin, health_score_at_firing, severity, fired_at)
SELECT $1, $2, severity, NOW()
FROM events
WHERE id = $3;
```

All three execute in one `async with conn.transaction():` block. If any write fails, roll back all three and re-raise.

---

### ContagionPropagator class

```python
class ContagionPropagator:
    def __init__(self, db_pool, health_scorer: HealthScorer):
        self.db = db_pool
        self.scorer = health_scorer
        self.MAX_DEPTH = 2

    async def propagate(
        self, cin: str, new_band: str, depth: int = 0
    ) -> List[str]:
        # Returns list of CINs that were re-scored.
        ...
```

#### propagate — step-by-step logic

```
PRECONDITION: called only when a band CHANGE has occurred (previous_band != new_band).
              Do not call on score changes that stay within the same band.

1.  If depth >= MAX_DEPTH (2): return [].

2.  Get all active DINs for cin from governance_graph.

3.  For each DIN, get all OTHER active CINs from governance_graph (exclude `cin` itself).
    Deduplicate. This is the set of "peer CINs".

4.  For each peer CIN:
    a.  Call self.scorer.recompute(peer_cin, triggering_event_id=synthetic_event_id).
        The triggering_event_id for contagion-triggered recomputes: insert a new row into
        events with event_type='CONTAGION_PROPAGATED', severity='WATCH',
        cin=peer_cin, source='CONTAGION_ENGINE', data_json includes:
            {
              "origin_cin": cin,
              "origin_new_band": new_band,
              "depth": depth,
              "chain": [list of CINs processed so far]
            }
        Use the id of this inserted event as the triggering_event_id for the recompute call.
    b.  If peer_cin's band changed (result.band != result.previous_band):
        i.  Append peer_cin to the returned list.
        ii. Recurse: await self.propagate(peer_cin, result.band, depth=depth+1).
        iii. Append recursion results to the returned list.
    c.  If band did NOT change: still append peer_cin to returned list (it was re-scored).

5.  After all peers processed: update the CONTAGION_PROPAGATED event row's data_json
    to include the full list of re-scored CINs (contagion_chain column on events).

6.  Return deduplicated list of all re-scored CINs.
```

**Important:** Do not re-score the same CIN twice within a single propagation wave. Maintain a `visited: set[str]` across the recursion (pass it as an internal parameter or use instance state scoped to the propagation call).

**CONTAGION_PROPAGATED event insert:**

```sql
INSERT INTO events (cin, source, event_type, severity, detected_at, data_json, contagion_checked)
VALUES ($1, 'CONTAGION_ENGINE', 'CONTAGION_PROPAGATED', 'WATCH', NOW(), $2::jsonb, TRUE)
RETURNING id;
```

---

## Shell company detection

### When to run

Call `ShellDetector.check(cin)` in two situations:
1. After any event with `event_type = 'NEW_COMPANY'` fires for a CIN.
2. After any event with `event_type = 'DIRECTOR_APPOINTED'` fires for a CIN — but only if `master_entities.date_of_incorporation >= NOW() - INTERVAL '36 months'` (skip the check for old companies).

### Six conditions — ALL must be true simultaneously

| # | Condition | Table + column |
|---|---|---|
| 1 | `date_of_incorporation >= NOW() - INTERVAL '36 months'` | `master_entities` |
| 2 | `paid_up_capital < 1000000` (less than ₹10 lakh, i.e., 1,000,000 paise is wrong — use rupees: `< 1000000` where the column stores rupees) | `master_entities` |
| 3 | `date_of_last_agm IS NULL` | `master_entities` |
| 4 | No row exists in `identifier_map` where `cin = $cin AND identifier_type = 'EPFO'` | `identifier_map` |
| 5 | No row exists in `identifier_map` where `cin = $cin AND identifier_type = 'GSTIN'` | `identifier_map` |
| 6 | At least one active director on this CIN (`governance_graph` where `cin=$cin AND is_active=TRUE`) is also active on 5 or more OTHER CINs | `governance_graph` |

Condition 6 SQL:

```sql
SELECT gg_outer.din
FROM governance_graph gg_outer
WHERE gg_outer.cin = $1
  AND gg_outer.is_active = TRUE
  AND (
    SELECT COUNT(DISTINCT gg_inner.cin)
    FROM governance_graph gg_inner
    WHERE gg_inner.din = gg_outer.din
      AND gg_inner.is_active = TRUE
      AND gg_inner.cin != $1
  ) >= 5
LIMIT 1;
```

If this returns any row, condition 6 is met.

### When all 6 are true — fire event

```sql
INSERT INTO events (
  cin, source, event_type, severity, detected_at, data_json
) VALUES (
  $1,
  'SHELL_DETECTOR',
  'SHELL_RISK',
  'WATCH',
  NOW(),
  $2::jsonb
);
```

`data_json` must include:
```json
{
  "conditions_met": [1, 2, 3, 4, 5, 6],
  "paid_up_capital": <value>,
  "date_of_incorporation": "<ISO date>",
  "director_with_board_count": "<DIN>",
  "other_board_count": <int>
}
```

### When fewer than 6 conditions are true — do nothing. No event, no log entry.

### ShellDetector class

```python
class ShellDetector:
    def __init__(self, db_pool):
        self.db = db_pool

    async def check(self, cin: str) -> bool:
        # Returns True if SHELL_RISK was fired, False otherwise.
        ...

    async def _check_all_conditions(self, cin: str) -> tuple[bool, dict]:
        # Returns (all_conditions_met, conditions_detail_dict)
        ...
```

---

## Sector cluster detection

### When to run

Called once per OGD diff run, after all company score recomputes from that diff are complete. This is a monthly operation (triggered by the OGD ingestion pipeline, not by individual events).

### Detection query

```sql
SELECT registered_state, industrial_class, COUNT(*) AS stressed_count,
       ARRAY_AGG(cin) AS affected_cins
FROM master_entities
WHERE health_band IN ('AMBER', 'RED')
  AND last_score_computed_at > NOW() - INTERVAL '30 days'
  AND status = 'Active'
GROUP BY registered_state, industrial_class
HAVING COUNT(*) >= 5;
```

### Deduplication check — do NOT re-fire if already fired within 30 days

For each cluster row returned:

```sql
SELECT id
FROM events
WHERE event_type = 'SECTOR_CLUSTER_ALERT'
  AND detected_at > NOW() - INTERVAL '30 days'
  AND data_json->>'registered_state' = $1
  AND data_json->>'industrial_class' = $2
LIMIT 1;
```

If this returns a row: skip. Do not fire a duplicate alert.

### Fire event

```sql
INSERT INTO events (
  cin, source, event_type, severity, detected_at, data_json
) VALUES (
  NULL,
  'SECTOR_CLUSTER_DETECTOR',
  'SECTOR_CLUSTER_ALERT',
  'WATCH',
  NOW(),
  $1::jsonb
);
```

`cin` is NULL for cluster events — they are not company-specific. `data_json` must include:

```json
{
  "registered_state": "<state code>",
  "industrial_class": "<NIC code>",
  "stressed_count": <int>,
  "affected_cins": ["CIN1", "CIN2", ...],
  "detection_window_days": 30
}
```

### SectorClusterDetector class

```python
class SectorClusterDetector:
    def __init__(self, db_pool):
        self.db = db_pool

    async def run(self) -> int:
        # Returns count of new cluster alerts fired.
        ...
```

---

## Score persistence — full sequence

After every `HealthScorer.recompute()` call, `_persist_score` executes three writes atomically:

### Write 1 — update master_entities

```sql
UPDATE master_entities
SET
  health_score           = $1,   -- final_score as SMALLINT
  health_band            = $2,   -- 'GREEN' | 'AMBER' | 'RED'
  last_score_computed_at = NOW()
WHERE cin = $3;
```

### Write 2 — update triggering event

```sql
UPDATE events
SET
  health_score_before = $1,  -- previous_score as SMALLINT
  health_score_after  = $2   -- final_score as SMALLINT
WHERE id = $3;
```

### Write 3 — insert into predictions

```sql
INSERT INTO predictions (
  cin,
  health_score_at_firing,
  severity,
  fired_at
)
SELECT
  $1,          -- cin
  $2,          -- final_score
  severity,    -- from the triggering event row
  NOW()
FROM events
WHERE id = $3;
```

All three writes are wrapped in a single `async with conn.transaction():` block. Rollback on any failure.

---

## Complete Python implementation skeleton

Codex must produce working code. The skeletons below define all signatures, import structure, and patterns. Fill in all method bodies.

### detection/health_scorer.py

```python
"""
Health scoring engine.

Recomputes the composite health score for a company (CIN) from local DB data only.
Called on every event that changes underlying company state. Never called on a schedule.

Architecture: HealthScorer → ContagionPropagator (calls HealthScorer recursively, max depth 2)
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, date
from typing import List, Dict, Any, Optional

import asyncpg

logger = logging.getLogger(__name__)

WEIGHTS = {
    "filing_freshness":   0.25,
    "director_stability": 0.20,
    "legal_risk":         0.25,
    "financial_health":   0.20,
    "capital_trajectory": 0.10,
}

ACTIVE_LEGAL_STATUSES_EXCLUDE = frozenset([
    "Disposed", "Dismissed", "Withdrawn", "Closed"
])


@dataclass
class ScoreResult:
    cin: str
    score: float
    band: str
    previous_score: float
    previous_band: str
    components: Dict[str, Dict[str, float]]
    contagion_penalty: float
    contagion_sources: List[str]
    triggering_event_id: int
    computed_at: datetime


class HealthScorer:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def recompute(self, cin: str, triggering_event_id: int) -> ScoreResult:
        """
        Main entry point. Reads all component data, computes raw score,
        applies contagion penalty, persists, returns ScoreResult.
        """
        async with self.db.acquire() as conn:
            # Read previous score from master_entities
            row = await conn.fetchrow(
                "SELECT health_score, health_band FROM master_entities WHERE cin = $1",
                cin
            )
            previous_score = float(row["health_score"]) if row and row["health_score"] is not None else 50.0
            previous_band = row["health_band"] if row and row["health_band"] else "AMBER"

        # Compute all five components concurrently
        (
            filing_freshness,
            director_stability,
            legal_risk,
            financial_health,
            capital_trajectory,
        ) = await asyncio.gather(
            self._compute_filing_freshness(cin),
            self._compute_director_stability(cin),
            self._compute_legal_risk(cin),
            self._compute_financial_health(cin),
            self._compute_capital_trajectory(cin),
        )

        raw_scores = {
            "filing_freshness":   filing_freshness,
            "director_stability": director_stability,
            "legal_risk":         legal_risk,
            "financial_health":   financial_health,
            "capital_trajectory": capital_trajectory,
        }

        raw_score = sum(raw_scores[k] * WEIGHTS[k] for k in WEIGHTS)

        final_score, contagion_penalty, contagion_sources = await self._apply_contagion_penalty(
            cin, raw_score
        )

        final_score = round(final_score, 1)
        band = await self._get_band(final_score)

        components = {
            k: {
                "raw": raw_scores[k],
                "weight": WEIGHTS[k],
                "weighted": round(raw_scores[k] * WEIGHTS[k], 4),
            }
            for k in WEIGHTS
        }

        await self._persist_score(
            cin, final_score, band, components,
            triggering_event_id, previous_score, previous_band
        )

        return ScoreResult(
            cin=cin,
            score=final_score,
            band=band,
            previous_score=previous_score,
            previous_band=previous_band,
            components=components,
            contagion_penalty=contagion_penalty,
            contagion_sources=contagion_sources,
            triggering_event_id=triggering_event_id,
            computed_at=datetime.utcnow(),
        )

    async def _compute_filing_freshness(self, cin: str) -> float:
        """
        Returns 0–100 based on age of last AGM filing.
        NULL + Active → 30. NULL + not Active → 0.
        """
        row = await self.db.fetchrow(
            "SELECT date_of_last_agm, status FROM master_entities WHERE cin = $1",
            cin
        )
        if row is None:
            return 30.0

        agm_date: Optional[date] = row["date_of_last_agm"]
        status: str = row["status"]

        if agm_date is None:
            return 30.0 if status == "Active" else 0.0

        months_ago = (date.today() - agm_date).days / 30.44

        if months_ago < 12:
            return 100.0
        elif months_ago < 18:
            return 70.0
        elif months_ago < 24:
            return 40.0
        elif months_ago < 36:
            return 15.0
        else:
            return 0.0

    async def _compute_director_stability(self, cin: str) -> float:
        """
        Returns 0–100 based on director appointment/cessation changes in last 90 days.
        """
        row = await self.db.fetchrow(
            """
            SELECT COUNT(*) AS change_count
            FROM governance_graph
            WHERE cin = $1
              AND (
                date_of_appointment >= NOW() - INTERVAL '90 days'
                OR cessation_date >= NOW() - INTERVAL '90 days'
              )
            """,
            cin
        )
        count = int(row["change_count"]) if row else 0

        if count == 0:
            return 100.0
        elif count == 1:
            return 80.0
        elif count == 2:
            return 50.0
        else:
            return 20.0

    async def _compute_legal_risk(self, cin: str) -> float:
        """
        Returns 0–100 based on active legal cases.
        Evaluation order: SARFAESI/NCLT → total count ≥3 → SEC_138 → single non-138 → zero.
        """
        rows = await self.db.fetch(
            """
            SELECT case_type, COUNT(*) AS cnt
            FROM legal_events
            WHERE cin = $1
              AND status NOT IN ('Disposed', 'Dismissed', 'Withdrawn', 'Closed')
            GROUP BY case_type
            """,
            cin
        )

        if not rows:
            return 100.0

        case_counts: Dict[str, int] = {r["case_type"]: int(r["cnt"]) for r in rows}
        total_active = sum(case_counts.values())

        # Highest-priority checks first
        sarfaesi_nclt_types = {
            "SARFAESI_AUCTION", "SARFAESI_13_4",
            "NCLT_7", "NCLT_9", "NCLT_10"
        }
        if any(t in case_counts for t in sarfaesi_nclt_types):
            return 5.0

        if total_active >= 3:
            return 20.0

        sec138_count = case_counts.get("SEC_138", 0)
        if 1 <= sec138_count <= 2:
            return 50.0

        if total_active == 1 and sec138_count == 0:
            return 80.0

        return 100.0

    async def _compute_financial_health(self, cin: str) -> float:
        """
        Returns 0–100 based on D/E ratio from latest financial snapshot.
        Returns 50 (neutral) if no data.
        """
        row = await self.db.fetchrow(
            """
            SELECT debt_to_equity
            FROM financial_snapshots
            WHERE cin = $1
            ORDER BY financial_year DESC
            LIMIT 1
            """,
            cin
        )

        if row is None or row["debt_to_equity"] is None:
            return 50.0

        de = float(row["debt_to_equity"])

        if de < 1.0:
            return 100.0
        elif de <= 2.0:
            return 70.0
        elif de <= 4.0:
            return 40.0
        else:
            return 15.0

    async def _compute_capital_trajectory(self, cin: str) -> float:
        """
        Returns 0–100 based on change in paid_up_capital vs previous OGD snapshot.
        Returns 50 (neutral) if no history.
        """
        current_row = await self.db.fetchrow(
            "SELECT paid_up_capital FROM master_entities WHERE cin = $1",
            cin
        )
        if current_row is None or current_row["paid_up_capital"] is None:
            return 50.0

        current_capital = int(current_row["paid_up_capital"])

        prev_row = await self.db.fetchrow(
            """
            SELECT data_json->>'previous_paid_up_capital' AS prev_capital
            FROM events
            WHERE cin = $1
              AND event_type IN ('CAPITAL_CHANGED', 'OGD_UPDATED')
              AND data_json ? 'previous_paid_up_capital'
            ORDER BY detected_at DESC
            LIMIT 1
            """,
            cin
        )

        if prev_row is None or prev_row["prev_capital"] is None:
            return 50.0

        try:
            previous_capital = int(prev_row["prev_capital"])
        except (ValueError, TypeError):
            return 50.0

        if previous_capital == 0:
            return 50.0

        pct_change = ((current_capital - previous_capital) / previous_capital) * 100

        if pct_change > 5:
            return 100.0
        elif pct_change >= -5:
            return 60.0
        else:
            return 20.0

    async def _apply_contagion_penalty(
        self, cin: str, raw_score: float
    ) -> tuple[float, float, list[str]]:
        """
        Looks up director-connected companies, accumulates RED/AMBER penalty.
        Returns (final_score, penalty_applied, contributing_cins).
        """
        din_rows = await self.db.fetch(
            "SELECT din FROM governance_graph WHERE cin = $1 AND is_active = TRUE",
            cin
        )
        if not din_rows:
            return (max(0.0, min(100.0, raw_score)), 0.0, [])

        dins = [r["din"] for r in din_rows]

        peer_rows = await self.db.fetch(
            """
            SELECT DISTINCT gg.cin, me.health_band
            FROM governance_graph gg
            JOIN master_entities me ON me.cin = gg.cin
            WHERE gg.din = ANY($1::varchar[])
              AND gg.is_active = TRUE
              AND gg.cin != $2
            """,
            dins, cin
        )

        penalty = 0.0
        contributing_cins: list[str] = []

        for row in peer_rows:
            peer_band = row["health_band"]
            peer_cin = row["cin"]
            if peer_band == "RED":
                penalty += 15.0
                contributing_cins.append(peer_cin)
            elif peer_band == "AMBER":
                penalty += 5.0
                contributing_cins.append(peer_cin)

        final_score = max(0.0, min(100.0, raw_score - penalty))
        return (final_score, penalty, contributing_cins)

    async def _get_band(self, score: float) -> str:
        if score >= 70.0:
            return "GREEN"
        elif score >= 40.0:
            return "AMBER"
        else:
            return "RED"

    async def _persist_score(
        self,
        cin: str,
        score: float,
        band: str,
        components: dict,
        event_id: int,
        previous_score: float,
        previous_band: str,
    ) -> None:
        """
        Three writes in one transaction:
        1. UPDATE master_entities
        2. UPDATE events (triggering event)
        3. INSERT predictions
        """
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE master_entities
                    SET health_score = $1,
                        health_band  = $2,
                        last_score_computed_at = NOW()
                    WHERE cin = $3
                    """,
                    int(score), band, cin
                )

                await conn.execute(
                    """
                    UPDATE events
                    SET health_score_before = $1,
                        health_score_after  = $2
                    WHERE id = $3
                    """,
                    int(previous_score), int(score), event_id
                )

                await conn.execute(
                    """
                    INSERT INTO predictions (cin, health_score_at_firing, severity, fired_at)
                    SELECT $1, $2, severity, NOW()
                    FROM events
                    WHERE id = $3
                    """,
                    cin, int(score), event_id
                )


class ContagionPropagator:
    """
    When a company's health band changes, propagates re-scoring to
    all director-connected companies. Maximum recursion depth: 2.
    """

    MAX_DEPTH = 2

    def __init__(self, db_pool: asyncpg.Pool, health_scorer: HealthScorer):
        self.db = db_pool
        self.scorer = health_scorer

    async def propagate(
        self,
        cin: str,
        new_band: str,
        depth: int = 0,
        _visited: Optional[set] = None,
    ) -> List[str]:
        """
        Returns list of CINs that were re-scored as a result of contagion.
        Only called when cin's band has changed (previous_band != new_band).
        """
        if _visited is None:
            _visited = set()

        if depth >= self.MAX_DEPTH:
            return []

        _visited.add(cin)
        rescored: List[str] = []

        din_rows = await self.db.fetch(
            "SELECT din FROM governance_graph WHERE cin = $1 AND is_active = TRUE",
            cin
        )
        if not din_rows:
            return []

        dins = [r["din"] for r in din_rows]

        peer_rows = await self.db.fetch(
            """
            SELECT DISTINCT cin
            FROM governance_graph
            WHERE din = ANY($1::varchar[])
              AND is_active = TRUE
              AND cin != $2
            """,
            dins, cin
        )

        peer_cins = [r["cin"] for r in peer_rows if r["cin"] not in _visited]

        for peer_cin in peer_cins:
            if peer_cin in _visited:
                continue

            _visited.add(peer_cin)

            # Insert contagion event to get a triggering_event_id
            contagion_event_id = await self.db.fetchval(
                """
                INSERT INTO events (cin, source, event_type, severity, detected_at,
                                    data_json, contagion_checked)
                VALUES ($1, 'CONTAGION_ENGINE', 'CONTAGION_PROPAGATED', 'WATCH',
                        NOW(), $2::jsonb, TRUE)
                RETURNING id
                """,
                peer_cin,
                {
                    "origin_cin": cin,
                    "origin_new_band": new_band,
                    "depth": depth,
                }
            )

            result = await self.scorer.recompute(peer_cin, contagion_event_id)
            rescored.append(peer_cin)

            # If band changed on peer, recurse
            if result.band != result.previous_band:
                sub_rescored = await self.propagate(
                    peer_cin, result.band, depth=depth + 1, _visited=_visited
                )
                rescored.extend(sub_rescored)

        # Update the contagion event with full chain info
        # (best-effort, do not fail the propagation if this update fails)
        try:
            await self.db.execute(
                """
                UPDATE events
                SET contagion_chain = $1::jsonb
                WHERE cin = $2
                  AND event_type = 'CONTAGION_PROPAGATED'
                  AND source = 'CONTAGION_ENGINE'
                  AND detected_at > NOW() - INTERVAL '1 minute'
                """,
                {"rescored_cins": rescored, "depth": depth},
                cin
            )
        except Exception as e:
            logger.warning("Failed to update contagion_chain: %s", e)

        return rescored
```

---

### detection/shell_detector.py

```python
"""
Shell company detector.

Checks six structural conditions simultaneously. Fires SHELL_RISK (WATCH severity)
only when ALL six conditions are true. No partial events.

Called after:
  - NEW_COMPANY event
  - DIRECTOR_APPOINTED event on a company incorporated within last 36 months
"""

from __future__ import annotations

import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


class ShellDetector:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def check(self, cin: str) -> bool:
        """
        Evaluates all six shell conditions.
        Returns True if SHELL_RISK event was fired, False otherwise.
        """
        all_met, detail = await self._check_all_conditions(cin)
        if not all_met:
            return False

        await self.db.execute(
            """
            INSERT INTO events (cin, source, event_type, severity, detected_at, data_json)
            VALUES ($1, 'SHELL_DETECTOR', 'SHELL_RISK', 'WATCH', NOW(), $2::jsonb)
            """,
            cin,
            {
                "conditions_met": [1, 2, 3, 4, 5, 6],
                "paid_up_capital": detail["paid_up_capital"],
                "date_of_incorporation": detail["date_of_incorporation"],
                "director_with_board_count": detail["director_din"],
                "other_board_count": detail["other_board_count"],
            }
        )
        logger.info("SHELL_RISK fired for CIN %s", cin)
        return True

    async def _check_all_conditions(self, cin: str) -> tuple[bool, dict]:
        """
        Returns (all_conditions_met, detail_dict).
        Short-circuits on first False condition.
        """
        # Conditions 1, 2, 3 — from master_entities
        row = await self.db.fetchrow(
            """
            SELECT date_of_incorporation, paid_up_capital, date_of_last_agm
            FROM master_entities
            WHERE cin = $1
            """,
            cin
        )
        if row is None:
            return False, {}

        detail: dict = {}

        # Condition 1: incorporated within 36 months
        if row["date_of_incorporation"] is None:
            return False, {}
        from datetime import date
        months_since_inc = (date.today() - row["date_of_incorporation"]).days / 30.44
        if months_since_inc > 36:
            return False, {}
        detail["date_of_incorporation"] = str(row["date_of_incorporation"])

        # Condition 2: paid_up_capital < 1,000,000 (₹10 lakh)
        if row["paid_up_capital"] is None or row["paid_up_capital"] >= 1_000_000:
            return False, {}
        detail["paid_up_capital"] = int(row["paid_up_capital"])

        # Condition 3: no AGM ever held
        if row["date_of_last_agm"] is not None:
            return False, {}

        # Condition 4: no EPFO record
        epfo_exists = await self.db.fetchval(
            """
            SELECT 1 FROM identifier_map
            WHERE cin = $1 AND identifier_type = 'EPFO'
            LIMIT 1
            """,
            cin
        )
        if epfo_exists:
            return False, {}

        # Condition 5: no GSTIN record
        gstin_exists = await self.db.fetchval(
            """
            SELECT 1 FROM identifier_map
            WHERE cin = $1 AND identifier_type = 'GSTIN'
            LIMIT 1
            """,
            cin
        )
        if gstin_exists:
            return False, {}

        # Condition 6: any director on this CIN is also active on 5+ other CINs
        director_row = await self.db.fetchrow(
            """
            SELECT gg_outer.din,
              (
                SELECT COUNT(DISTINCT gg_inner.cin)
                FROM governance_graph gg_inner
                WHERE gg_inner.din = gg_outer.din
                  AND gg_inner.is_active = TRUE
                  AND gg_inner.cin != $1
              ) AS other_board_count
            FROM governance_graph gg_outer
            WHERE gg_outer.cin = $1
              AND gg_outer.is_active = TRUE
            ORDER BY other_board_count DESC
            LIMIT 1
            """,
            cin
        )

        if director_row is None or int(director_row["other_board_count"]) < 5:
            return False, {}

        detail["director_din"] = director_row["din"]
        detail["other_board_count"] = int(director_row["other_board_count"])

        return True, detail
```

---

### detection/sector_cluster.py

```python
"""
Sector cluster detector.

Runs once per OGD diff cycle (monthly). Detects when 5+ active companies
in the same state + NIC code have moved to AMBER/RED within the last 30 days.

Deduplicates: will not re-fire if the same state+NIC cluster was alerted
within the last 30 days.
"""

from __future__ import annotations

import logging

import asyncpg

logger = logging.getLogger(__name__)


class SectorClusterDetector:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def run(self) -> int:
        """
        Executes cluster detection. Returns count of new cluster alerts fired.
        """
        rows = await self.db.fetch(
            """
            SELECT registered_state,
                   industrial_class,
                   COUNT(*) AS stressed_count,
                   ARRAY_AGG(cin) AS affected_cins
            FROM master_entities
            WHERE health_band IN ('AMBER', 'RED')
              AND last_score_computed_at > NOW() - INTERVAL '30 days'
              AND status = 'Active'
            GROUP BY registered_state, industrial_class
            HAVING COUNT(*) >= 5
            """
        )

        fired_count = 0

        for row in rows:
            state = row["registered_state"]
            nic = row["industrial_class"]

            # Deduplication check
            already_fired = await self.db.fetchval(
                """
                SELECT id FROM events
                WHERE event_type = 'SECTOR_CLUSTER_ALERT'
                  AND detected_at > NOW() - INTERVAL '30 days'
                  AND data_json->>'registered_state' = $1
                  AND data_json->>'industrial_class' = $2
                LIMIT 1
                """,
                state, nic
            )

            if already_fired:
                logger.debug(
                    "Sector cluster for state=%s nic=%s already fired recently, skipping.",
                    state, nic
                )
                continue

            affected_cins = list(row["affected_cins"])

            await self.db.execute(
                """
                INSERT INTO events (
                  cin, source, event_type, severity, detected_at, data_json
                ) VALUES (
                  NULL,
                  'SECTOR_CLUSTER_DETECTOR',
                  'SECTOR_CLUSTER_ALERT',
                  'WATCH',
                  NOW(),
                  $1::jsonb
                )
                """,
                {
                    "registered_state": state,
                    "industrial_class": nic,
                    "stressed_count": int(row["stressed_count"]),
                    "affected_cins": affected_cins,
                    "detection_window_days": 30,
                }
            )

            logger.info(
                "SECTOR_CLUSTER_ALERT fired: state=%s nic=%s count=%d",
                state, nic, row["stressed_count"]
            )
            fired_count += 1

        return fired_count
```

---

### tests/test_health_scorer.py

```python
"""
Pytest suite for health scoring engine.
All DB interactions are mocked. No real DB required.

Run: pytest tests/test_health_scorer.py -v
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

from detection.health_scorer import HealthScorer, ScoreResult, ContagionPropagator
from detection.shell_detector import ShellDetector
from detection.sector_cluster import SectorClusterDetector


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_pool(fetch_map: Dict[str, Any] = None, fetchrow_map: Dict = None,
              fetchval_map: Dict = None) -> MagicMock:
    """
    Returns a mock asyncpg pool. Callers configure return values by inspecting
    the SQL passed to fetch/fetchrow/fetchval in each test.
    """
    pool = MagicMock()
    pool.fetch = AsyncMock(return_value=[])
    pool.fetchrow = AsyncMock(return_value=None)
    pool.fetchval = AsyncMock(return_value=None)
    pool.execute = AsyncMock(return_value=None)

    # acquire() returns an async context manager that yields the pool itself
    # (simplified — full connection is not needed for unit tests)
    conn = MagicMock()
    conn.fetchrow = pool.fetchrow
    conn.fetch = pool.fetch
    conn.fetchval = pool.fetchval
    conn.execute = pool.execute
    conn.transaction = MagicMock(return_value=_AsyncNullContextManager())

    acquire_cm = _AsyncNullContextManager(conn)
    pool.acquire = MagicMock(return_value=acquire_cm)

    return pool


class _AsyncNullContextManager:
    """Async context manager that returns a given value."""
    def __init__(self, value=None):
        self.value = value

    async def __aenter__(self):
        return self.value

    async def __aexit__(self, *args):
        pass


def run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# Test 1: Filing overdue >18 months → freshness score ≤ 40
# ---------------------------------------------------------------------------

def test_filing_freshness_overdue_18_months():
    """
    AGM date 20 months ago → 18–24 month band → freshness score = 40.
    """
    pool = make_pool()
    agm_date = date.today() - timedelta(days=20 * 30)
    pool.fetchrow = AsyncMock(return_value={"date_of_last_agm": agm_date, "status": "Active"})

    scorer = HealthScorer(pool)
    score = run(scorer._compute_filing_freshness("TEST001"))

    assert score <= 40.0, f"Expected score ≤ 40 for 20-month overdue AGM, got {score}"
    assert score == 40.0


# ---------------------------------------------------------------------------
# Test 2: 3 director changes in 90 days → stability score = 20
# ---------------------------------------------------------------------------

def test_director_stability_three_changes():
    """
    3 director change events in last 90 days → stability score = 20.
    """
    pool = make_pool()
    pool.fetchrow = AsyncMock(return_value={"change_count": 3})

    scorer = HealthScorer(pool)
    score = run(scorer._compute_director_stability("TEST001"))

    assert score == 20.0


# ---------------------------------------------------------------------------
# Test 3: NCLT filing → legal_risk score = 5
# ---------------------------------------------------------------------------

def test_legal_risk_nclt_filing():
    """
    One active NCLT_7 case → legal_risk score = 5.
    """
    pool = make_pool()
    pool.fetch = AsyncMock(return_value=[{"case_type": "NCLT_7", "cnt": 1}])

    scorer = HealthScorer(pool)
    score = run(scorer._compute_legal_risk("TEST001"))

    assert score == 5.0


# ---------------------------------------------------------------------------
# Test 4: SARFAESI possession → legal_risk score = 5
# ---------------------------------------------------------------------------

def test_legal_risk_sarfaesi_possession():
    """
    Active SARFAESI_13_4 (possession) case → legal_risk score = 5.
    """
    pool = make_pool()
    pool.fetch = AsyncMock(return_value=[{"case_type": "SARFAESI_13_4", "cnt": 1}])

    scorer = HealthScorer(pool)
    score = run(scorer._compute_legal_risk("TEST001"))

    assert score == 5.0


# ---------------------------------------------------------------------------
# Test 5: D/E ratio 3.0 → financial_health score = 40
# ---------------------------------------------------------------------------

def test_financial_health_de_ratio_3():
    """
    D/E = 3.0 falls in the 2–4 band → financial_health score = 40.
    """
    pool = make_pool()
    pool.fetchrow = AsyncMock(return_value={"debt_to_equity": 3.0})

    scorer = HealthScorer(pool)
    score = run(scorer._compute_financial_health("TEST001"))

    assert score == 40.0


# ---------------------------------------------------------------------------
# Test 6: No financial data → financial_health score = 50 (neutral)
# ---------------------------------------------------------------------------

def test_financial_health_no_data():
    """
    No financial snapshot → neutral score = 50.
    """
    pool = make_pool()
    pool.fetchrow = AsyncMock(return_value=None)

    scorer = HealthScorer(pool)
    score = run(scorer._compute_financial_health("TEST001"))

    assert score == 50.0


# ---------------------------------------------------------------------------
# Test 7: Capital increased 10% → trajectory score = 100
# ---------------------------------------------------------------------------

def test_capital_trajectory_increased_10_pct():
    """
    Current capital 110, previous 100 → +10% → trajectory score = 100.
    """
    pool = make_pool()

    # fetchrow will be called twice: once for master_entities, once for events
    pool.fetchrow = AsyncMock(side_effect=[
        {"paid_up_capital": 1_100_000},      # master_entities
        {"prev_capital": "1000000"},          # events table
    ])

    scorer = HealthScorer(pool)
    score = run(scorer._compute_capital_trajectory("TEST001"))

    assert score == 100.0


# ---------------------------------------------------------------------------
# Test 8: Contagion — 2 RED director connections → penalty = 30
# ---------------------------------------------------------------------------

def test_contagion_two_red_directors():
    """
    Two connected CINs with health_band='RED' → penalty = 30 → raw 70 becomes 40.
    """
    pool = make_pool()

    # Active DINs for subject CIN
    pool.fetch = AsyncMock(side_effect=[
        [{"din": "DIN001"}, {"din": "DIN002"}],   # call 1: get DINs
        [                                           # call 2: get peer CINs+bands
            {"cin": "PEER001", "health_band": "RED"},
            {"cin": "PEER002", "health_band": "RED"},
        ],
    ])

    scorer = HealthScorer(pool)
    final_score, penalty, sources = run(scorer._apply_contagion_penalty("TEST001", 70.0))

    assert penalty == 30.0
    assert final_score == 40.0
    assert set(sources) == {"PEER001", "PEER002"}


# ---------------------------------------------------------------------------
# Test 9: Score floored at 0 even with heavy contagion
# ---------------------------------------------------------------------------

def test_contagion_floor_at_zero():
    """
    Raw score 10, penalty 100 (many RED peers) → final score = 0, not negative.
    """
    pool = make_pool()
    pool.fetch = AsyncMock(side_effect=[
        [{"din": "DIN001"}],
        [{"cin": f"PEER{i:03d}", "health_band": "RED"} for i in range(10)],
    ])

    scorer = HealthScorer(pool)
    final_score, penalty, _ = run(scorer._apply_contagion_penalty("TEST001", 10.0))

    assert final_score == 0.0
    assert penalty == 150.0  # 10 RED peers * 15


# ---------------------------------------------------------------------------
# Test 10: Band change AMBER→RED triggers ContagionPropagator
# ---------------------------------------------------------------------------

def test_band_change_triggers_contagion(monkeypatch):
    """
    When recompute produces a band change from AMBER to RED,
    ContagionPropagator.propagate() should be called.
    """
    pool = make_pool()
    propagator_called_with = []

    async def mock_propagate(cin, new_band, depth=0, _visited=None):
        propagator_called_with.append((cin, new_band))
        return []

    # Build a scorer that will produce a RED result from previously AMBER
    scorer = HealthScorer(pool)

    # Patch _persist_score to no-op
    scorer._persist_score = AsyncMock(return_value=None)

    # Control component scores to produce RED (score ~18)
    scorer._compute_filing_freshness   = AsyncMock(return_value=0.0)
    scorer._compute_director_stability = AsyncMock(return_value=20.0)
    scorer._compute_legal_risk         = AsyncMock(return_value=5.0)
    scorer._compute_financial_health   = AsyncMock(return_value=50.0)
    scorer._compute_capital_trajectory = AsyncMock(return_value=20.0)

    # Previous state: AMBER
    pool.fetchrow = AsyncMock(return_value={"health_score": 55, "health_band": "AMBER"})

    # Contagion: no connected peers (test the call, not the propagation itself)
    pool.fetch = AsyncMock(return_value=[])

    propagator = ContagionPropagator(pool, scorer)
    propagator.propagate = mock_propagate

    result = run(scorer.recompute("TEST001", triggering_event_id=99))

    assert result.band == "RED"
    assert result.previous_band == "AMBER"
    # The caller (event router) is responsible for calling propagate when band changes.
    # HealthScorer itself does NOT call ContagionPropagator — that is the router's job.
    # This test verifies the band change is correctly reported in ScoreResult.
    assert result.band != result.previous_band


# ---------------------------------------------------------------------------
# Test 11: Contagion depth stops at 2
# ---------------------------------------------------------------------------

def test_contagion_depth_limit():
    """
    ContagionPropagator.propagate() with depth=2 returns [] immediately.
    """
    pool = make_pool()
    scorer = HealthScorer(pool)
    propagator = ContagionPropagator(pool, scorer)

    result = run(propagator.propagate("TEST001", "RED", depth=2))

    assert result == []
    # No DB calls should have been made
    pool.fetch.assert_not_called()


# ---------------------------------------------------------------------------
# Test 12: Shell detection — all 6 conditions met → SHELL_RISK fired
# ---------------------------------------------------------------------------

def test_shell_detection_all_conditions_met():
    """
    When all 6 shell conditions are simultaneously true, SHELL_RISK event is inserted.
    """
    pool = make_pool()
    today = date.today()
    inception = today - timedelta(days=365)  # 12 months old

    call_count = 0

    async def mock_fetchrow(sql, *args):
        nonlocal call_count
        call_count += 1
        if "master_entities" in sql:
            return {
                "date_of_incorporation": inception,
                "paid_up_capital": 500_000,     # < 1,000,000
                "date_of_last_agm": None,
            }
        if "other_board_count" in sql or "governance_graph" in sql:
            return {"din": "DIN001", "other_board_count": 7}
        return None

    async def mock_fetchval(sql, *args):
        # EPFO check → no record
        if "EPFO" in sql:
            return None
        # GSTIN check → no record
        if "GSTIN" in sql:
            return None
        return None

    pool.fetchrow = mock_fetchrow
    pool.fetchval = mock_fetchval

    detector = ShellDetector(pool)
    fired = run(detector.check("TEST001"))

    assert fired is True
    pool.execute.assert_called_once()
    call_args = pool.execute.call_args
    assert "SHELL_RISK" in call_args[0][0]


# ---------------------------------------------------------------------------
# Test 13: Shell detection — 5 of 6 conditions met → no event
# ---------------------------------------------------------------------------

def test_shell_detection_five_of_six_conditions():
    """
    GSTIN is present → condition 5 fails → no SHELL_RISK event.
    """
    pool = make_pool()
    today = date.today()
    inception = today - timedelta(days=365)

    async def mock_fetchrow(sql, *args):
        if "master_entities" in sql:
            return {
                "date_of_incorporation": inception,
                "paid_up_capital": 500_000,
                "date_of_last_agm": None,
            }
        if "governance_graph" in sql:
            return {"din": "DIN001", "other_board_count": 7}
        return None

    async def mock_fetchval(sql, *args):
        if "EPFO" in sql:
            return None
        if "GSTIN" in sql:
            return 1  # GSTIN EXISTS — condition 5 fails

    pool.fetchrow = mock_fetchrow
    pool.fetchval = mock_fetchval

    detector = ShellDetector(pool)
    fired = run(detector.check("TEST001"))

    assert fired is False
    pool.execute.assert_not_called()


# ---------------------------------------------------------------------------
# Test 14: Sector cluster — 5 stressed companies same state+NIC → alert fires
# ---------------------------------------------------------------------------

def test_sector_cluster_fires_when_threshold_met():
    """
    5 AMBER/RED companies in same state+NIC within 30 days → alert fires.
    No prior alert in last 30 days.
    """
    pool = make_pool()

    cluster_rows = [
        {
            "registered_state": "GJ",
            "industrial_class": "2410",
            "stressed_count": 5,
            "affected_cins": ["C1", "C2", "C3", "C4", "C5"],
        }
    ]
    pool.fetch = AsyncMock(return_value=cluster_rows)
    pool.fetchval = AsyncMock(return_value=None)  # no prior alert

    detector = SectorClusterDetector(pool)
    count = run(detector.run())

    assert count == 1
    pool.execute.assert_called_once()
    insert_call = pool.execute.call_args[0][0]
    assert "SECTOR_CLUSTER_ALERT" in insert_call


# ---------------------------------------------------------------------------
# Test 15: Sector cluster — already fired 15 days ago → not re-fired
# ---------------------------------------------------------------------------

def test_sector_cluster_deduplication():
    """
    Cluster already fired 15 days ago → dedup check returns existing event id
    → no new insert.
    """
    pool = make_pool()

    cluster_rows = [
        {
            "registered_state": "GJ",
            "industrial_class": "2410",
            "stressed_count": 6,
            "affected_cins": ["C1", "C2", "C3", "C4", "C5", "C6"],
        }
    ]
    pool.fetch = AsyncMock(return_value=cluster_rows)
    pool.fetchval = AsyncMock(return_value=42)  # existing event found → skip

    detector = SectorClusterDetector(pool)
    count = run(detector.run())

    assert count == 0
    pool.execute.assert_not_called()
```

---

## Caller contract — who calls what

The event router (FastAPI background task or Celery worker) is responsible for orchestrating the scoring and propagation sequence. HealthScorer and ContagionPropagator do not call each other automatically except within ContagionPropagator (which calls HealthScorer.recompute for each peer).

**Sequence when an event fires:**

```
1.  EventRouter receives new event (event_id, cin, event_type)
2.  EventRouter calls: result = await health_scorer.recompute(cin, event_id)
3.  If result.band != result.previous_band:
        rescored = await contagion_propagator.propagate(cin, result.band)
4.  EventRouter proceeds to routing/watchlist matching with result
```

**Shell detector call points:**

```
After NEW_COMPANY event:
    await shell_detector.check(event.cin)

After DIRECTOR_APPOINTED event:
    me = await db.fetchrow("SELECT date_of_incorporation FROM master_entities WHERE cin=$1", cin)
    if me and (date.today() - me.date_of_incorporation).days <= 36 * 30:
        await shell_detector.check(event.cin)
```

**Sector cluster call point:**

```
After OGD diff ingestion completes (monthly):
    count = await sector_cluster_detector.run()
```

---

## Error handling rules

1. Any exception in a component method (`_compute_*`) must be caught, logged at ERROR level, and return `50.0` (neutral) so the overall score still computes.
2. Any exception in `_apply_contagion_penalty` must be caught, logged at ERROR level, and return `(raw_score, 0.0, [])` — never block score persistence.
3. Any exception in `_persist_score` must propagate — a score computed but not persisted is a data integrity failure. Let it bubble up.
4. ContagionPropagator exceptions per-peer must be caught and logged. One failed peer re-score must not block the others.
5. ShellDetector and SectorClusterDetector exceptions must not propagate to the caller. Catch at class level, log, return safe values (False / 0).

---

## Performance notes

- The five `_compute_*` methods run with `asyncio.gather` — do not add any blocking I/O inside them.
- The contagion peer query uses `din = ANY($1::varchar[])` with the DIN array from step 1 — this is a single query, not N queries per DIN. The index `idx_governance_graph_din_active` in SCHEMA_SPEC.md covers this pattern.
- `SectorClusterDetector.run()` runs a single aggregation query against master_entities. The index `idx_master_entities_health_band` covers the `health_band IN ('AMBER', 'RED')` filter.
- At 18 lakh+ companies, the shell detector runs per-CIN on events, not batch. Never call `ShellDetector.check()` in a loop over all companies.

---

## What Codex must NOT do

- Do not call any external API (CompData, GST portal, Claude API) from any method in these files.
- Do not schedule score recomputes on a timer or cron. Scores are event-triggered only.
- Do not pre-generate AI summaries from within this layer.
- Do not modify the scoring weights (WEIGHTS dict) without a spec change from Claude Code.
- Do not add a `WATCH` band to `ScoreResult.band` — WATCH is an overlay applied by the router, not a score output.
- Do not retry DB queries silently. Log and return neutral values as specified in the error handling rules.
