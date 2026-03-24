# Gemini Handoff — Phase 1 Dashboard Scaffold

## 1. Your Role

You are building the React/Next.js frontend for a corporate signal intelligence dashboard. The backend is FastAPI, built separately. In Phase 1, you build the complete UI scaffold with realistic mock data — no live API connections yet. Claude Code will provide API contracts as the backend is ready.

The product monitors 35 Indian government sources for corporate events — NCLT filings, SARFAESI notices, director resignations, AGM defaults, and similar signals. Every company resolves to a CIN (Corporate Identification Number). Events are scored by severity: CRITICAL, ALERT, WATCH, INFO.

---

## 2. Tech Stack

- Next.js 14 (App Router)
- TypeScript
- Tailwind CSS
- shadcn/ui component library
- Recharts (score timelines and charts)
- Zustand (global state)
- SWR (for future data fetching — wire up now, mock responses)
- lucide-react (icons)

---

## 3. Design System

Dark theme only. Define all colors as CSS variables in `globals.css` and extend them in `tailwind.config.ts`.

```css
:root {
  --bg:        #0a0a0a;
  --surface:   #111111;
  --border:    #1f1f1f;
  --text:      #f5f5f5;
  --muted:     #6b7280;
  --critical:  #ef4444;
  --alert:     #f97316;
  --watch:     #eab308;
  --info:      #6b7280;
  --green:     #22c55e;
  --amber:     #f59e0b;
  --red:       #ef4444;
}
```

Font: Inter from Google Fonts. Apply globally via `layout.tsx`.

Tailwind config must map all CSS variables to semantic names:

```ts
// tailwind.config.ts (extend.colors)
bg: 'var(--bg)',
surface: 'var(--surface)',
border-subtle: 'var(--border)',
text-primary: 'var(--text)',
text-muted: 'var(--muted)',
critical: 'var(--critical)',
alert-orange: 'var(--alert)',
watch: 'var(--watch)',
info: 'var(--info)',
band-green: 'var(--green)',
band-amber: 'var(--amber)',
band-red: 'var(--red)',
```

All cards: `bg-surface border border-subtle rounded-lg`. No drop shadows. No white backgrounds anywhere.

---

## 4. Pages

### /dashboard — Live Event Feed

Top stats bar: 4 cards in a horizontal row.
- CRITICAL events in last 24h (red accent)
- ALERT events in last 24h (orange accent)
- WATCH events in last 24h (yellow accent)
- Total companies monitored (neutral)

Event feed: full-width table below the stats bar.

Columns:
- Severity (colored pill badge)
- Company Name
- CIN (monospace, muted)
- Event Type
- Score Change (e.g. `52 → 28`, red if decreased)
- Source (monospace tag)
- Time Ago

Filters row above table:
- Severity dropdown (All / CRITICAL / ALERT / WATCH / INFO)
- Source dropdown (All / mca_ogd / nclt / sarfaesi / ecourts / ibbi / drt / mca_directors / sebi_deals)
- Time range dropdown (Last 1h / Last 6h / Last 24h)

All filters are client-side. Filter interactions must visibly update the table immediately.

Auto-refresh indicator top-right of page: "Last updated 14s ago" — increment every second using `setInterval`. Do not actually re-fetch in Phase 1; just update the counter.

Mock data: 15–20 events. Mix severities. Include at minimum: 2 NCLT filings, 2 SARFAESI notices, 1 AGM overdue, 2 director resignations, 1 SEBI order, 1 IBBI liquidation, 3 MCA filing defaults. Use realistic company names, CINs, and score changes.

---

### /companies/[cin] — Company Profile

Use mock CIN `U27100GJ2015PTC082456` as the default resolved profile for any `[cin]` route in Phase 1.

Header section:
- Company name: "Gujarat Positra Steel Pvt Ltd"
- CIN badge (monospace pill)
- Status pill: "Active" (green) or "Struck Off" (red)
- State and NIC sector in muted text

Health score widget:
- Large number display (e.g. `28`)
- Colored band label beneath it: GREEN (80–100), AMBER (50–79), RED (0–49)
- Sparkline: Recharts `LineChart` with last 12 data points, no axes labels, just the trend curve. Color matches band.
- This company: score 28, RED band.

Score breakdown: horizontal `BarChart` (Recharts) showing 5 components, each as a separate bar with its weighted contribution score:
- Filing Freshness
- Director Stability
- Legal Risk
- Financial Health
- Capital Trajectory

Recent events timeline: vertical list, last 10 events. Each item: severity icon (colored dot) + event type + source tag + time ago. Most recent at top.

Active directors table:
- Columns: DIN | Name | Designation | Date Appointed | Other Boards

Active legal cases table:
- Columns: Case Type | Case Number | Court | Filing Date | Amount (₹)
- This company: 3 active Sec 138 NI Act cases.

Mock data: Also prepare 2 additional company profiles (one AMBER ~65 score, one GREEN ~88 score) — these can be accessed by hardcoding alternate CINs that redirect back to mock data.

---

### /watchlists — Watchlist Manager

Two-panel layout (left 280px fixed, right flex-grow).

Left panel:
- Header "Watchlists" with a "New" button (+ icon)
- List of saved watchlists. Each row: name, short filter summary (e.g. "Maharashtra · Sec 138 · ALERT+"), active/inactive toggle switch.
- Selecting a row loads it in the right panel.

Right panel (create / edit form):
- Name (text input)
- CIN List (textarea, one CIN per line, placeholder: "U27100GJ2015PTC082456")
- State filter (dropdown — all Indian states + UTs)
- Sector filter (text input, NIC code or keyword)
- Minimum severity (dropdown: CRITICAL / ALERT / WATCH / INFO)
- Signal types (multi-select checkboxes):
  - NCLT Filing
  - SARFAESI Notice
  - Director Resignation
  - AGM Default
  - MCA Strike-Off
  - IBBI Liquidation
  - SEBI Order
  - DRT Notice
- Save button / Cancel button

Mock data: 3 pre-existing watchlists with different filter combinations.

---

### /sources — Source Monitor

Full-width table.

Columns:
- Source Name (monospace)
- Status badge (OK = green, DEGRADED = yellow, UNREACHABLE = red)
- Last Pull (relative time, e.g. "4m ago")
- Record Count (integer, comma-separated)
- Lag (e.g. "2h 14m behind")
- Consecutive Failures (integer, red if > 0)

Sources to display (8):
- mca_ogd
- nclt
- drt
- sarfaesi
- ecourts
- mca_directors
- sebi_deals
- ibbi

Make at least 1 source DEGRADED and 1 UNREACHABLE in mock data.

Click a row to expand an inline detail section (accordion style):
- Last 5 pull timestamps (table: timestamp | records pulled | duration | status)
- Error log (monospace text area, last error message if status is not OK)

---

### /accuracy — Prediction Accuracy

Summary cards row (4 cards):
- Total RED alerts fired (last 30d)
- Confirmed (true positives)
- False positives
- Accuracy % (large, colored: green if > 75%, amber if 50–75%, red if < 50%)

Mock: 45 total, 32 confirmed, 13 false positives, 71% accuracy.

Line chart (Recharts `LineChart`): accuracy % over last 12 months. X axis = month label, Y axis = 0–100. Add a dashed horizontal reference line at 75% labeled "Target".

False positives table:
- Columns: Company | CIN | Event Type | Fired At | Reason (short text)
- Show last 10 false positive entries.

---

### /costs — Cost Monitor

Today's total cost header: large ₹ figure (e.g. `₹142.50`) with a progress bar toward ₹500 daily threshold.

If total > ₹400: show a yellow warning banner at top of page — "Approaching daily threshold (₹500). Operator review will trigger at limit."

Cost breakdown table:
- Columns: Service | Operation | Units | Unit Cost | Total ₹
- Mock rows: Claude API (entity resolution, 420 calls, ₹0.02, ₹8.40), 2captcha (CAPTCHA solves, 15 solves, ₹0.10, ₹1.50), CompData (enrichment lookups, 37 lookups, ₹3.58, ₹132.60)

7-day cost trend: Recharts `BarChart`, one bar per day, last 7 days. X axis = date. Color bars amber if > ₹300, green otherwise. Mock: vary values realistically across the week.

---

## 5. Shared Layout

Root layout wraps all pages with:
- Left sidebar (240px wide, collapsible to 56px icon-rail on toggle)
- Main content area (flex-grow, `bg-bg`, `min-h-screen`)
- Sticky top bar

### Sidebar

- Top: App name "BI Engine" with a `Radio` or `Radar` lucide icon. Hidden in collapsed state (icon only).
- Nav links (with lucide icons):
  - Dashboard (`LayoutDashboard`) — with a red pill badge showing CRITICAL count
  - Companies (`Building2`)
  - Watchlists (`List`)
  - Sources (`Server`)
  - Accuracy (`Target`)
  - Costs (`IndianRupee`)
- Active link: left accent border (2px, color matches severity accent or white), slightly brighter background.
- Bottom of sidebar: version string `v0.1.0` and `Phase 1` label in muted text.

### Top Bar

Sticky, `bg-surface border-b border-subtle`, height 56px.

Contents (left to right):
- Page title (dynamic, matches current route)
- Spacer
- 3 severity summary badges: `CRITICAL 3` (red), `ALERT 7` (orange), `WATCH 12` (yellow)
- "Last synced 2m ago" in muted text
- Dark/light toggle icon button (`Moon` / `Sun` icon) — dark is default. Light mode optional; if implemented, swap CSS variables. If not implemented, button is visible but no-ops.

---

## 6. Loading and Empty States

Every page must implement all three states:

**Skeleton loader**: Use shadcn/ui `Skeleton` component. Simulate a 1.2s loading delay on mount using `useState` + `useEffect` + `setTimeout`. Show skeleton shapes that match the actual layout (table rows, card blocks, chart placeholder).

**Empty state**: Shown when filters return zero results. Centered in the content area: a `SearchX` lucide icon, heading "No results", subtext matching context (e.g. "No events match the selected filters."). No button needed unless it makes sense (e.g. "Clear filters" on /dashboard).

**Error state**: Shown when a fetch would fail. Centered: `AlertCircle` icon, heading "Failed to load", subtext "Something went wrong. Try again.", and a "Retry" button that re-triggers the loading simulation.

---

## 7. File Structure

Build exactly this structure:

```
bi-dashboard/
├── app/
│   ├── globals.css
│   ├── layout.tsx                  ← root layout, sidebar + topbar
│   ├── page.tsx                    ← redirect to /dashboard
│   ├── dashboard/
│   │   └── page.tsx
│   ├── companies/
│   │   └── [cin]/
│   │       └── page.tsx
│   ├── watchlists/
│   │   └── page.tsx
│   ├── sources/
│   │   └── page.tsx
│   ├── accuracy/
│   │   └── page.tsx
│   └── costs/
│       └── page.tsx
├── components/
│   ├── layout/
│   │   ├── Sidebar.tsx
│   │   └── TopBar.tsx
│   ├── dashboard/
│   │   ├── EventFeed.tsx
│   │   ├── EventRow.tsx
│   │   └── SeverityBadge.tsx
│   ├── company/
│   │   ├── HealthScoreWidget.tsx
│   │   ├── ScoreBreakdown.tsx
│   │   └── EventTimeline.tsx
│   ├── ui/                         ← shadcn auto-generated components
│   └── shared/
│       ├── SkeletonLoader.tsx
│       ├── EmptyState.tsx
│       └── ErrorState.tsx
├── lib/
│   ├── mock-data.ts                ← all mock data, single source of truth
│   ├── types.ts                    ← all TypeScript interfaces
│   └── utils.ts                    ← cn(), formatScore(), timeAgo(), formatCurrency()
├── store/
│   └── useAppStore.ts              ← Zustand: filters, selected watchlist, sidebar collapsed state
└── tailwind.config.ts
```

Do not deviate from this structure. Claude Code will reference these paths when wiring in API contracts.

---

## 8. TypeScript Types (`lib/types.ts`)

Define the following interfaces. These are the canonical shapes. Backend will conform to these in Phase 2.

```ts
export type Severity = 'CRITICAL' | 'ALERT' | 'WATCH' | 'INFO';
export type SourceStatus = 'OK' | 'DEGRADED' | 'UNREACHABLE';
export type CompanyStatus = 'Active' | 'Struck Off' | 'Under Liquidation';
export type Band = 'GREEN' | 'AMBER' | 'RED';

export interface Event {
  id: string;
  cin: string;
  companyName: string;
  eventType: string;
  severity: Severity;
  source: string;
  scoreBefore: number;
  scoreAfter: number;
  firedAt: string;           // ISO timestamp
  description: string;
}

export interface ScoreComponents {
  filingFreshness: number;
  directorStability: number;
  legalRisk: number;
  financialHealth: number;
  capitalTrajectory: number;
}

export interface HealthScore {
  current: number;
  band: Band;
  history: { date: string; score: number }[];  // last 12 points
  components: ScoreComponents;
}

export interface Director {
  din: string;
  name: string;
  designation: string;
  dateAppointed: string;
  otherBoards: number;
}

export interface LegalCase {
  caseType: string;
  caseNumber: string;
  court: string;
  filingDate: string;
  amountInr: number | null;
}

export interface Company {
  cin: string;
  name: string;
  status: CompanyStatus;
  state: string;
  nicSector: string;
  healthScore: HealthScore;
  recentEvents: Event[];
  directors: Director[];
  legalCases: LegalCase[];
}

export interface Watchlist {
  id: string;
  name: string;
  cinList: string[];
  stateFilter: string | null;
  sectorFilter: string | null;
  minSeverity: Severity;
  signalTypes: string[];
  active: boolean;
}

export interface SourceRecord {
  id: string;
  name: string;
  status: SourceStatus;
  lastPullAt: string;
  recordCount: number;
  lagMinutes: number;
  consecutiveFailures: number;
  pullHistory: {
    timestamp: string;
    recordsPulled: number;
    durationMs: number;
    status: SourceStatus;
  }[];
  lastError: string | null;
}

export interface CostEntry {
  date: string;
  service: string;
  operation: string;
  units: number;
  unitCostInr: number;
  totalInr: number;
}

export interface DailyCost {
  date: string;
  totalInr: number;
}

export interface PredictionAccuracy {
  period: string;           // e.g. "2025-02"
  totalAlerts: number;
  confirmed: number;
  falsePositives: number;
  accuracyPct: number;
}

export interface FalsePositive {
  id: string;
  cin: string;
  companyName: string;
  eventType: string;
  firedAt: string;
  reason: string;
}
```

---

## 9. Mock Data (`lib/mock-data.ts`)

Build all mock data in a single file. Export named constants. No external imports except from `./types`.

Requirements:

**Events (15–20 entries)**
- Mix: CRITICAL × 3, ALERT × 5, WATCH × 6, INFO × 3 minimum
- Sources must include: nclt, sarfaesi, mca_ogd, ecourts, ibbi, mca_directors, sebi_deals
- Event types: "NCLT Filing — Winding Up", "SARFAESI Notice — Section 13(2)", "AGM Default — FY2024", "Director Resignation", "MCA Filing Default — AOC-4", "IBBI Liquidation Order", "SEBI Settlement Order", "DRT Notice — Recovery", "Strike-Off Warning"
- Score changes: CRITICAL should show drops > 20 points. WATCH drops < 10.
- Use real-format CINs: `U[5digit NIC][2-letter state][4-digit year][PTC/PLC/OPC][6-digit number]`

**Companies (3 profiles)**
- RED company: CIN `U27100GJ2015PTC082456`, "Gujarat Positra Steel Pvt Ltd", score 28, 3 Sec138 cases, 4 directors, 10 recent events
- AMBER company: CIN `U74999MH2011PTC218765`, "Ashvita Logistics Pvt Ltd", score 65, 1 legal case, 3 directors, 5 recent events
- GREEN company: CIN `U62099DL2018PLC334421`, "Indra Nexus Technologies Pvt Ltd", score 88, 0 legal cases, 5 directors, 2 recent events

**Sources (8 entries)**
- mca_ogd: OK, last pull 4m ago, 1,842,310 records, 0 lag, 0 failures
- nclt: OK, last pull 12m ago, 48,221 records, 10m lag, 0 failures
- drt: DEGRADED, last pull 2h ago, 12,445 records, 118m lag, 2 failures
- sarfaesi: OK, last pull 8m ago, 9,887 records, 6m lag, 0 failures
- ecourts: DEGRADED, last pull 45m ago, 284,112 records, 40m lag, 1 failure
- mca_directors: OK, last pull 6m ago, 4,221,008 records, 4m lag, 0 failures
- sebi_deals: UNREACHABLE, last pull 6h ago, 7,234 records, 360m lag, 5 failures
- ibbi: OK, last pull 18m ago, 3,109 records, 15m lag, 0 failures

**Watchlists (3 entries)**
- "Maharashtra Borrowers" — state: Maharashtra, severity: ALERT, signals: SARFAESI Notice, DRT Notice
- "Steel Sector Watch" — NIC: 2710, severity: WATCH, signals: NCLT Filing, IBBI Liquidation Order
- "Strike-Off Risk" — severity: CRITICAL, signals: MCA Filing Default, Strike-Off Warning, AGM Default

**Daily costs (7 days)**
- Vary between ₹80 and ₹280, with today at ₹142.50
- Today breakdown: Claude API ₹8.40, 2captcha ₹1.50, CompData ₹132.60

**Accuracy (12 months)**
- Vary between 62% and 79%, trend slightly upward
- Last month: 45 alerts, 32 confirmed, 13 false positives, 71%
- Include 10 false positive entries with realistic reasons (e.g. "Director resignation was a board restructure, not distress signal", "NCLT filing withdrawn within 48h")

---

## 10. Zustand Store (`store/useAppStore.ts`)

```ts
interface AppStore {
  // Dashboard filters
  severityFilter: Severity | 'ALL';
  sourceFilter: string;
  timeRangeFilter: '1h' | '6h' | '24h';
  setSeverityFilter: (v: Severity | 'ALL') => void;
  setSourceFilter: (v: string) => void;
  setTimeRangeFilter: (v: '1h' | '6h' | '24h') => void;

  // Sidebar
  sidebarCollapsed: boolean;
  toggleSidebar: () => void;

  // Watchlists
  selectedWatchlistId: string | null;
  setSelectedWatchlistId: (id: string | null) => void;

  // Global severity counts (derived from mock events, set on mount)
  criticalCount: number;
  alertCount: number;
  watchCount: number;
  setCounts: (c: number, a: number, w: number) => void;
}
```

---

## 11. Utility Functions (`lib/utils.ts`)

Implement:
- `cn(...classes)` — clsx + tailwind-merge for className merging
- `timeAgo(isoString: string): string` — returns "4m ago", "2h ago", "3d ago"
- `formatScore(score: number): string` — returns "28" zero-padded to nothing, just the number
- `getBand(score: number): Band` — GREEN if >= 80, AMBER if >= 50, RED otherwise
- `getBandColor(band: Band): string` — returns Tailwind text color class
- `formatCurrency(inr: number): string` — returns "₹142.50"
- `formatLag(minutes: number): string` — returns "2h 14m" or "45m"

---

## 12. Deliverables Checklist

Before handing back:

- [ ] `npm run dev` starts without errors on `localhost:3000`
- [ ] All 6 pages render with mock data visible (no blank screens)
- [ ] `npm run build` completes with zero TypeScript errors
- [ ] Sidebar navigation works between all pages, active state correct
- [ ] Sidebar collapses to icon rail on toggle
- [ ] Dashboard filters (severity, source, time range) all filter the event table client-side
- [ ] Auto-refresh counter increments every second
- [ ] /companies/[cin] renders full profile with chart and tables
- [ ] Recharts charts render on /companies/[cin], /accuracy, /costs
- [ ] Skeleton loaders appear for ~1.2s on every page before content
- [ ] Empty state renders when all dashboard filters exclude all events
- [ ] Error state component exists (can be triggered by a dev toggle or prop)
- [ ] No `any` TypeScript types in non-generated files
- [ ] All colors sourced from CSS variables — no hardcoded hex values in components
- [ ] File structure matches exactly what is specified above
