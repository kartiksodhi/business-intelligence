# API_CONTRACTS.md

## Owner: Claude Code
## Consumer: Gemini (dashboard)
## Last updated: 2026-03-16
## Base URL: `NEXT_PUBLIC_API_URL` env var (e.g. `http://localhost:8000`)

All endpoints are under `/op/`. No auth in Phase 1. All responses are JSON.

---

## Conventions

- Timestamps: ISO 8601 strings, UTC, e.g. `"2026-03-16T10:00:00"`
- Nulls: optional fields are `null` when absent — never omitted
- Errors: `{ "detail": "message string" }` with appropriate HTTP status
- Health bands: `"GREEN"` | `"AMBER"` | `"RED"` | `"WATCH"` | `null`
- Severity: `"INFO"` | `"WATCH"` | `"ALERT"` | `"CRITICAL"`
- Source status: `"OK"` | `"DEGRADED"` | `"UNREACHABLE"` | `"SCRAPER_BROKEN"`

---

## GET /op/status

**Purpose:** Source monitor page — scraper health table.

**Response:** `Array<SourceStatusItem>`

```ts
interface SourceStatusItem {
  source_id: string           // e.g. "mca_ogd", "nclt", "drt"
  status: string              // "OK" | "DEGRADED" | "UNREACHABLE" | "SCRAPER_BROKEN"
  last_pull_at: string | null // ISO timestamp
  record_count: number | null // rows pulled on last run
  consecutive_failures: number
  next_pull_at: string | null // ISO timestamp
  lag_hours: number | null    // hours overdue; null if not yet overdue
}
```

**Example:**
```json
[
  {
    "source_id": "mca_ogd",
    "status": "OK",
    "last_pull_at": "2026-03-16T06:00:00",
    "record_count": 1823456,
    "consecutive_failures": 0,
    "next_pull_at": "2026-03-17T06:00:00",
    "lag_hours": null
  },
  {
    "source_id": "nclt",
    "status": "DEGRADED",
    "last_pull_at": "2026-03-15T12:00:00",
    "record_count": 48200,
    "consecutive_failures": 2,
    "next_pull_at": "2026-03-16T08:00:00",
    "lag_hours": 14.5
  }
]
```

---

## GET /op/events/today

**Purpose:** Event feed on dashboard home. Supports filtering.

**Query params:**
| Param | Type | Default | Description |
|---|---|---|---|
| `severity` | string | none | Filter to one severity level |
| `source` | string | none | Filter to one source_id |
| `limit` | number | 100 | Max rows, 1–500 |

**Response:** `Array<EventItem>`

```ts
interface EventItem {
  id: number
  cin: string | null             // null if entity not yet resolved
  source: string
  event_type: string             // e.g. "NCLT_SEC7_FILED", "STATUS_CHANGE"
  severity: string
  detected_at: string            // ISO timestamp
  health_score_before: number | null
  health_score_after: number | null
  data_json_summary: string      // first 200 chars of raw data_json, for preview
}
```

**Example:**
```json
[
  {
    "id": 1042,
    "cin": "U27100GJ2015PTC082456",
    "source": "nclt",
    "event_type": "NCLT_SEC7_FILED",
    "severity": "CRITICAL",
    "detected_at": "2026-03-16T09:14:00",
    "health_score_before": 45,
    "health_score_after": 18,
    "data_json_summary": "{\"case_number\": \"CP/45/2026\", \"bench\": \"NCLT Mumbai\"}"
  }
]
```

---

## GET /op/health/{cin}

**Purpose:** Company profile page — full health view.

**Path param:** `cin` — 21-character CIN string

**Response:** `CompanyHealthResponse`

```ts
interface HealthComponent {
  raw: number       // 0–100 raw sub-score
  weighted: number  // raw * weight, 4 decimal places
}

interface CompanyHealthResponse {
  cin: string
  company_name: string
  health_score: number          // 0–100
  health_band: string | null    // "GREEN" | "AMBER" | "RED" | "WATCH"
  last_computed_at: string | null  // ISO timestamp of last score recompute

  components: {
    filing_freshness:    HealthComponent  // weight 0.25
    director_stability:  HealthComponent  // weight 0.20
    legal_risk:          HealthComponent  // weight 0.25
    financial_health:    HealthComponent  // weight 0.20
    capital_trajectory:  HealthComponent  // weight 0.10
  }

  recent_events: Array<EventItem>         // last 10 events for this CIN

  active_legal_cases: Array<{
    id: number
    case_type: string     // "NCLT_7" | "NCLT_9" | "DRT" | "SARFAESI_13_2" | etc.
    case_number: string | null
    court: string | null
    filing_date: string | null  // "YYYY-MM-DD"
    status: string | null
    amount_involved: number | null  // in INR
  }>

  directors: Array<{
    din: string
    director_name: string | null
    designation: string | null
    date_of_appointment: string | null  // "YYYY-MM-DD"
    is_active: boolean
  }>
}
```

**404 response:**
```json
{ "detail": "CIN U99999XX2000PTC000000 not found." }
```

**Note:** `components` raw values come from the most recent `HEALTH_SCORE_COMPUTED` event's `data_json.components`. If no such event exists, all raw values are `0.0`.

---

## GET /op/sources/lag

**Purpose:** Source monitor — overdue sources only, sorted by worst lag.

**Response:** `Array<SourceLagItem>`

```ts
interface SourceLagItem {
  source_id: string
  expected_next_pull: string | null  // ISO timestamp
  actual_lag_hours: number           // hours overdue (always > 0 in this list)
  status: string
}
```

**Example:**
```json
[
  {
    "source_id": "drt",
    "expected_next_pull": "2026-03-15T08:00:00",
    "actual_lag_hours": 26.3,
    "status": "UNREACHABLE"
  }
]
```

---

## GET /op/accuracy

**Purpose:** Prediction accuracy panel — feedback loop stats.

**Response:** `AccuracyResponse`

```ts
interface FalsePositiveCause {
  event_type: string  // most common event type among false positive predictions
  count: number
}

interface AccuracyResponse {
  window_days: number            // always 30
  total_red_alerts: number       // predictions fired with health_score_at_firing <= 33
  confirmed: number
  false_positives: number
  expired_unconfirmed: number
  accuracy_pct: number           // confirmed / (confirmed + false_positives) * 100
  top_false_positive_causes: Array<FalsePositiveCause>  // up to 5
}
```

**Example:**
```json
{
  "window_days": 30,
  "total_red_alerts": 45,
  "confirmed": 32,
  "false_positives": 8,
  "expired_unconfirmed": 5,
  "accuracy_pct": 80.0,
  "top_false_positive_causes": [
    { "event_type": "SEC138_FILED", "count": 4 },
    { "event_type": "STATUS_CHANGE", "count": 2 }
  ]
}
```

---

## GET /op/costs/today

**Purpose:** Cost monitor — today's API spend vs ₹500 threshold.

**Response:** `CostsTodayResponse`

```ts
interface CostBreakdownItem {
  service: string           // "claude_api" | "2captcha" | "compdata" | etc.
  operation: string | null  // e.g. "alert_summary", "entity_resolution"
  units: number | null      // API calls made
  cost_inr: number
}

interface CostsTodayResponse {
  date: string                 // "YYYY-MM-DD"
  total_inr: number
  breakdown: Array<CostBreakdownItem>
  alert_threshold_inr: number  // always 500.0 in Phase 1
  threshold_breached: boolean
}
```

**Example:**
```json
{
  "date": "2026-03-16",
  "total_inr": 47.20,
  "breakdown": [
    { "service": "claude_api", "operation": "alert_summary", "units": 12, "cost_inr": 1.20 },
    { "service": "2captcha",   "operation": "captcha_solve",  "units": 3,  "cost_inr": 46.00 }
  ],
  "alert_threshold_inr": 500.0,
  "threshold_breached": false
}
```

---

## POST /op/watchlist

**Purpose:** Create a new watchlist filter.

**Request body:**
```ts
interface WatchlistCreateRequest {
  name: string                    // required, non-empty
  cin_list?: string[] | null      // specific CINs to watch; null = all
  state_filter?: string | null    // 2-letter state code e.g. "MH", "GJ"
  sector_filter?: string | null   // NIC code e.g. "27100"
  severity_min?: string           // default "WATCH"
  signal_types?: string[] | null  // specific event types; null = all
}
```

**Response (201):** `WatchlistCreateResponse`
```ts
interface WatchlistCreateResponse {
  id: number
  name: string
  cin_list: string[] | null
  state_filter: string | null
  sector_filter: string | null
  severity_min: string
  signal_types: string[] | null
  is_active: boolean
  created_at: string  // ISO timestamp
}
```

---

## POST /op/enrich/{cin}

**Purpose:** Queue a CIN for CompData enrichment.

**Response (200):** `EnrichResponse`
```ts
interface EnrichResponse {
  cin: string
  status: "queued"
  message: string
}
```

**404** if CIN not in `master_entities`.

---

## POST /op/resolve

**Purpose:** Operator manually resolves an entity resolution queue item.

**Request body:**
```ts
interface ResolveRequest {
  queue_id: number
  resolved_cin: string
}
```

**Response (200):** `ResolveResponse`
```ts
interface ResolveResponse {
  queue_id: number
  resolved_cin: string
  message: string
}
```

**404** if `queue_id` not found.
**422** if `resolved_cin` does not exist in `master_entities`.

---

## POST /op/recalibrate

**Purpose:** Trigger a recalibration job.

**Response (200):**
```ts
interface RecalibrateResponse {
  status: "started"
  message: string
}
```

---

## POST /op/captcha/solve

**Purpose:** Submit a manual CAPTCHA solution for a blocked scraper.

**Request body:**
```ts
interface CaptchaSolveRequest {
  source_id: string
  solution: string
}
```

**Response (200):**
```ts
interface CaptchaSolveResponse {
  status: "accepted"
  source_id: string
}
```

---

## Error shapes

| Status | When | Shape |
|---|---|---|
| 404 | Resource not found | `{ "detail": "message" }` |
| 422 | Validation failed (bad CIN, bad request body) | `{ "detail": "message" }` or Pydantic validation array |
| 500 | DB error or unhandled exception | `{ "detail": { "error": "Internal server error." } }` |

---

## Polling cadence for dashboard

| Endpoint | Suggested poll interval |
|---|---|
| `GET /op/status` | 60 seconds |
| `GET /op/events/today` | 30 seconds |
| `GET /op/health/{cin}` | On demand (user navigation) |
| `GET /op/sources/lag` | 60 seconds |
| `GET /op/accuracy` | 5 minutes |
| `GET /op/costs/today` | 5 minutes |

WebSocket for real-time is Phase 2. Use polling for Phase 1.
