# PHASE3_BATCH1_SPEC.md

## What this is
Scraper and detection specs for Phase 3 Batch 1 — 8 highest-signal government sources.
All are government portals. No Firecrawl. No paid APIs.

## Hard constraints (from CLAUDE.md)
- Playwright before anything else. All sources here use Playwright.
- Scrape by recency not entity. Never iterate 18L companies.
- Diff not reprocess. Store state per source. Fire only on delta.
- Health score recomputes on event only, never on schedule.
- Deduplicate before alert. Batch multi-event alerts per CIN.

## Build order
1. ingestion/scrapers/gst.py
2. ingestion/scrapers/epfo.py
3. ingestion/scrapers/rbi_wilful_defaulter.py
4. ingestion/scrapers/sebi_enforcement.py
5. ingestion/scrapers/gem.py
6. ingestion/scrapers/dgft.py
7. ingestion/scrapers/udyam.py
8. ingestion/scrapers/rbi_nbfc.py
9. detection/signal_combiner.py
10. detection/shell_detector.py
11. Tests for each

---

## Source 1: GST Portal (Source #14)

**URL**: https://services.gst.gov.in/services/searchtp (taxpayer search by GSTIN)
**Cadence**: Weekly
**Prerequisite**: CIN→GSTIN mapping. Read from `master_entities.gstin` column. If NULL, skip — do not attempt to resolve.
**Scrape strategy**:
- Load monitored GSTINs from master_entities WHERE gstin IS NOT NULL AND status = 'Active'
- For each GSTIN, POST to the taxpayer search API or use Playwright on the search page
- Extract: gstin, trade_name, registration_date, gstin_status (Active/Cancelled/Suspended), cancellation_date, cancellation_reason
- Hash the status field. If hash unchanged since last_pull → skip. If changed → fire event.

**Events fired** (insert into legal_events):
- `GST_CANCELLED`: gstin_status changed to Cancelled on a company with MCA status=Active → CRITICAL (zombie signal)
- `GST_SUSPENDED`: gstin_status changed to Suspended → RED
- `GST_RESTORED`: gstin_status changed back to Active → GREEN

**source_state key**: `gst:{gstin}`
**entity resolution**: CIN already known from master_entities join. No resolution needed.

**CAPTCHA**: GST portal uses image CAPTCHA on web UI. Use the JSON API endpoint instead:
`https://services.gst.gov.in/services/api/search/taxpayerDetails?gstin={gstin}`
This returns JSON directly. No CAPTCHA.

**Stub condition**: If API returns 429 or blocks — implement exponential backoff, max 3 retries. If still blocked, log and return [] for that batch.

---

## Source 2: EPFO (Source #24)

**URL**: https://unifiedportal-mem.epfindia.gov.in/publicPortal/
**Cadence**: Monthly
**Prerequisite**: CIN→PF establishment number mapping. Read from `master_entities.epfo_id` column. If NULL, skip.
**Scrape strategy**:
- Load monitored establishments from master_entities WHERE epfo_id IS NOT NULL
- For each establishment, use Playwright to search the unified portal
- Extract: establishment_name, epfo_id, coverage_status (Covered/Exempted/Cancelled), latest_contribution_month, employee_count_proxy (member count if available)
- Store previous employee_count_proxy in source_state. Compute percentage change.

**Events fired**:
- `EPFO_CONTRIBUTION_DROP`: employee_count_proxy dropped >20% MoM → RED
- `EPFO_ESTABLISHMENT_DELISTED`: coverage_status changed to Cancelled → CRITICAL
- `EPFO_HIRING_SURGE`: employee_count_proxy increased >20% MoM → GREEN

**source_state key**: `epfo:{epfo_id}`
**entity resolution**: CIN already known. No resolution needed.

**CAPTCHA**: EPFO portal has CAPTCHAs. Use pytesseract for these — this is exactly the CAPTCHA use case, not a legal document. Route via `route_document('captcha', captcha_image)`.

**Stub condition**: If portal structure changed — log, return [], do not fail scheduler.

---

## Source 3: RBI Wilful Defaulter List (Source #19)

**URL**: https://www.rbi.org.in/Scripts/bs_viewcontent.aspx?Id=2691
Secondary: https://rbidocs.rbi.org.in/rdocs/content/PDFs/ (quarterly PDF releases)
**Cadence**: Quarterly
**Scrape strategy**:
- Playwright: load the RBI wilful defaulter page
- Extract the HTML table of defaulters (Name, PAN/CIN if available, Lender, Amount)
- Hash the full table content. If hash unchanged → skip.
- On change: diff new rows against stored list. Only fire on NEW entries, not existing ones.
- Entity resolve company names to CINs using EntityResolver.

**Events fired**:
- `WILFUL_DEFAULT_ADDED`: New entry in wilful defaulter list → CRITICAL (confirmed bad actor)
- `WILFUL_DEFAULT_DIRECTOR`: Director of a monitored company appears in list → RED (contagion)

**source_state key**: `rbi_wilful_defaulter:full_list`
**entity resolution**: Names from RBI table → EntityResolver. Confidence below 70 → unmapped_signals, not main tables.

**CAPTCHA**: RBI site has no CAPTCHA. Standard Playwright.

---

## Source 4: SEBI Enforcement Orders (Source #17)

**URL**: https://www.sebi.gov.in/enforcement/orders/
**Cadence**: Weekly
**Scrape strategy**:
- Playwright: load enforcement orders page, filter by current week's date range
- Extract: order_date, entity_name, order_type (penalty/debarment/investigation/settlement), order_text_url
- Recency-based: only pull orders from last 7 days. Never reprocess older orders.
- Hash each order by (entity_name + order_date + order_type). Fire only on new hashes.
- Entity resolve entity_name to CIN.

**Events fired**:
- `SEBI_PENALTY`: Penalty order against entity → RED
- `SEBI_DEBARMENT`: Trading or market restriction imposed → CRITICAL
- `SEBI_INVESTIGATION`: Investigation initiated → AMBER
- `SEBI_DIRECTOR_ACTION`: Order against director of a monitored company → RED (contagion)

**source_state key**: `sebi_enforcement:last_order_date`
**entity resolution**: EntityResolver on entity_name. Unresolved → unmapped_signals.

**CAPTCHA**: SEBI site has no CAPTCHA.

---

## Source 5: GeM (Source #22)

**URL**: https://gem.gov.in/
Seller search: https://gem.gov.in/seller/sellerdetails
Order data: https://bidplus.gem.gov.in/bidlists (public bid listing)
**Cadence**: Weekly
**Scrape strategy**:
- Playwright: load GeM public bid listing, filter by award date = last 7 days
- Extract: bid_number, title, buyer_org, seller_name, seller_gstin, order_value_inr, award_date
- Entity resolve seller_name to CIN. Also try seller_gstin → master_entities.gstin lookup first (faster).
- Hash by (bid_number). Fire only new bid_numbers.

**Events fired**:
- `GEM_ORDER_WON`: Order value above ₹50L (5000000) → GREEN (revenue signal)
- `GEM_LARGE_CONTRACT`: Order value above ₹1Cr (10000000) → GREEN (strong revenue signal)
- `GEM_NEW_SELLER`: Monitored company just appeared as GeM seller → GREEN

**source_state key**: `gem:last_award_date`
**entity resolution**: GSTIN lookup first, then EntityResolver on seller_name.

**CAPTCHA**: GeM has Cloudflare on some pages. Use realistic user agent + 2s delay. If blocked, stub and log.

---

## Source 6: DGFT (Source #15)

**URL**: https://www.dgft.gov.in/CP/?opt=iecipr (IEC public register)
**Cadence**: Monthly
**Scrape strategy**:
- Playwright: search IEC register for recent cancellations (filter by date = last 30 days)
- Extract: iec_code, entity_name, pan, status (Active/Cancelled/Surrendered), date_of_cancellation
- Entity resolve entity_name + pan to CIN. PAN lookup is fast — prefer it.
- Hash by (iec_code + status). Fire on status change only.

**Events fired**:
- `IEC_CANCELLED`: Import/export code cancelled → RED (stopped trading signal)
- `IEC_SURRENDERED`: Voluntarily surrendered → AMBER
- `IEC_NEW`: New IEC issued for monitored company → GREEN

**source_state key**: `dgft:{iec_code}`
**entity resolution**: PAN → master_entities.pan first. Fall back to EntityResolver on name.

**CAPTCHA**: DGFT has CAPTCHA on IEC search. pytesseract via route_document('captcha', img).

---

## Source 7: Udyam Registration (Source #31)

**URL**: https://udyamregistration.gov.in/UdyamRegistration.aspx
Public verification: https://udyamregistration.gov.in/Government-India/Central-Government-org/udyam-registration-number-verification.aspx
**Cadence**: Quarterly
**Scrape strategy**:
- Playwright: use the public verification endpoint to check status by Udyam number
- Prerequisite: CIN→Udyam number mapping from master_entities.udyam_id column
- Extract: udyam_no, enterprise_name, classification (Micro/Small/Medium), nic_code, registration_date, status (Active/Cancelled)
- Hash by (udyam_no + classification + status). Fire on change.

**Events fired**:
- `UDYAM_NEW`: New MSME registration (new entrant in market) → GREEN
- `UDYAM_CLASSIFICATION_UPGRADE`: Micro→Small or Small→Medium → GREEN (growth signal)
- `UDYAM_CANCELLED`: Registration cancelled → AMBER

**source_state key**: `udyam:{udyam_no}`
**entity resolution**: CIN known from master_entities join. No resolution needed.

**CAPTCHA**: Udyam has image CAPTCHA. pytesseract.

---

## Source 8: RBI NBFC/Bank Notifications (Source #35)

**URL**: https://www.rbi.org.in/Scripts/BS_PressReleaseDisplay.aspx
NBFC enforcement: https://www.rbi.org.in/Scripts/BS_NBFCList.aspx
**Cadence**: Weekly
**Scrape strategy**:
- Playwright: load RBI press releases, filter by category = "Enforcement Actions" or "Cancellation of CoR"
- Date filter: last 7 days only. Recency-based.
- Extract: press_release_date, title, entity_name, action_type (cancellation/penalty/restriction/warning)
- Hash by (press_release_date + entity_name + action_type). Fire on new hashes.
- Entity resolve entity_name to CIN.

**Events fired**:
- `RBI_LICENSE_CANCELLED`: CoR (Certificate of Registration) cancelled → CRITICAL
- `RBI_ENFORCEMENT`: Enforcement action / penalty → RED
- `RBI_RESTRICTION`: Business restrictions imposed → RED
- `RBI_WARNING`: Regulatory warning → AMBER

**source_state key**: `rbi_nbfc:last_release_date`
**entity resolution**: EntityResolver on entity_name. NBFC names are formal legal names — high resolution accuracy expected.

**CAPTCHA**: RBI site has no CAPTCHA.

---

## Detection Module 1: Signal Combiner

**File**: detection/signal_combiner.py
**Trigger**: Called by routing layer after EVERY new event is inserted into legal_events.
**Purpose**: Check if any signal combination from SOURCES.md matrix is now satisfied for the affected CIN.

**Combinations to implement** (from SOURCES.md signal matrix):

```python
COMBINATIONS = [
    {
        "name": "PRE_INSOLVENCY_CLASSIC",
        "severity": "RED",
        "conditions": [
            {"event_type": "FILING_DECAY"},       # AGM overdue >18mo
            {"event_type__in": ["ECOURTS_SEC138", "ECOURTS_CIVIL"]},
        ],
        "window_days": 180,
    },
    {
        "name": "NPA_SEIZURE_IMMINENT",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "FILING_DECAY"},
            {"event_type__in": ["SARFAESI_DEMAND", "SARFAESI_POSSESSION"]},
        ],
        "window_days": 90,
    },
    {
        "name": "ZOMBIE_COMPANY",
        "severity": "RED",
        "conditions": [
            {"event_type": "GST_CANCELLED"},
            {"source": "mca", "field": "status", "value": "Active"},
        ],
        "window_days": 365,
    },
    {
        "name": "OPERATIONAL_SHUTDOWN",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "EPFO_CONTRIBUTION_DROP"},
            {"event_type": "GEM_HIRING_FREEZE"},   # no new job postings (Phase 3 Batch 2)
            {"event_type": "FILING_DECAY"},
        ],
        "window_days": 90,
    },
    {
        "name": "GOVERNANCE_COLLAPSE",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "AUDITOR_CHANGED"},
            {"event_type": "CFO_RESIGNED"},
            {"event_type": "FILING_DECAY"},
        ],
        "window_days": 180,
    },
    {
        "name": "CONTAGION_BAD_ACTOR",
        "severity": "RED",
        "conditions": [
            {"event_type": "WILFUL_DEFAULT_DIRECTOR"},
        ],
        "window_days": 0,  # fires immediately, no window needed
    },
    {
        "name": "PROMOTER_EXIT_BANK_SEIZURE",
        "severity": "CRITICAL",
        "conditions": [
            {"event_type": "SEBI_BULK_DEAL_PROMOTER_SELL"},  # Phase 3 Batch 2
            {"event_type__in": ["SARFAESI_DEMAND", "SARFAESI_POSSESSION"]},
        ],
        "window_days": 90,
    },
    {
        "name": "FUNDED_GROWTH",
        "severity": "GREEN",
        "conditions": [
            {"event_type": "CAPITAL_INCREASE"},
            {"event_type": "EPFO_HIRING_SURGE"},
            {"event_type": "GEM_ORDER_WON"},
        ],
        "window_days": 180,
    },
]
```

**Logic**:
```python
def check_combinations(cin: str, new_event_type: str, db_conn) -> list[dict]:
    """
    Called after each new event. Returns list of combination alerts to fire.
    Only fires a combination if ALL conditions are met within window_days.
    Never fires the same combination for the same CIN twice within 30 days.
    """
```

**Output**: For each combination that fires, insert a new row into legal_events with:
- event_type = combination["name"]
- severity = combination["severity"]
- source = "signal_combiner"
- notes = JSON list of the constituent event IDs that triggered it

---

## Detection Module 2: Shell Detector

**File**: detection/shell_detector.py
**Trigger**: Monthly cron, runs after OGD diff is processed.
**Purpose**: Identify shell companies using pure boolean logic. No LLM, no external API.

**Formula** (from INTELLIGENCE.md Part 3 §4):
```
IF (Age_Months < 36)
AND (Auth_Capital <= 1000000)        # ≤ ₹10L
AND (AGM_Date IS NULL)
AND (EPFO_Active == False)           # no epfo_id, or EPFO_ESTABLISHMENT_DELISTED
AND (GSTIN_Active == False)          # no gstin, or GST_CANCELLED/GST_SUSPENDED
AND (Max_Director_Board_Count >= 5)  # any director of this company sits on 5+ boards
THEN fire SHELL_RISK
```

**SQL to implement**:
```sql
SELECT me.cin
FROM master_entities me
WHERE
    EXTRACT(MONTH FROM AGE(NOW(), me.date_of_incorporation)) < 36
    AND me.authorized_capital <= 1000000
    AND me.agm_date IS NULL
    AND (me.epfo_id IS NULL OR EXISTS (
        SELECT 1 FROM legal_events le
        WHERE le.cin = me.cin
          AND le.event_type = 'EPFO_ESTABLISHMENT_DELISTED'
          AND le.event_date > NOW() - INTERVAL '12 months'
    ))
    AND (me.gstin IS NULL OR EXISTS (
        SELECT 1 FROM legal_events le
        WHERE le.cin = me.cin
          AND le.event_type IN ('GST_CANCELLED', 'GST_SUSPENDED')
          AND le.event_date > NOW() - INTERVAL '12 months'
    ))
    AND EXISTS (
        SELECT 1 FROM director_graph dg
        WHERE dg.cin = me.cin
          AND (
              SELECT COUNT(DISTINCT dg2.cin)
              FROM director_graph dg2
              WHERE dg2.din = dg.din
          ) >= 5
    )
    AND NOT EXISTS (
        SELECT 1 FROM legal_events le
        WHERE le.cin = me.cin
          AND le.event_type = 'SHELL_RISK'
          AND le.event_date > NOW() - INTERVAL '30 days'
    )
```

**Event fired**: `SHELL_RISK` at WATCH severity. Insert into legal_events.

---

## Scheduler additions

Add all 8 new scrapers to ingestion/scheduler.py:
- gst: weekly (Monday 6am)
- epfo: monthly (1st of month, 7am)
- rbi_wilful_defaulter: quarterly (1st of Jan/Apr/Jul/Oct, 8am)
- sebi_enforcement: weekly (Tuesday 6am)
- gem: weekly (Wednesday 6am)
- dgft: monthly (5th of month, 7am)
- udyam: quarterly (same as rbi_wilful_defaulter)
- rbi_nbfc: weekly (Thursday 6am)

Shell detector: monthly (last day of month, 11pm — after OGD diff)
Signal combiner: not scheduled — called inline by routing layer after each event insert.

---

## Pass criteria

- pytest tests/ — all 33 existing tests pass + new tests pass
- python ingestion/scheduler.py starts without error
- shell_detector.py runs against bi_engine database without error (0 results is fine)
- signal_combiner.check_combinations() returns [] when no combination conditions met
- No scraper iterates over master_entities — all scrape by recency/date filter

## Known unknowns — probe before writing selector

1. GST JSON API: verify `https://services.gst.gov.in/services/api/search/taxpayerDetails?gstin=07AABCU9603R1ZP` returns JSON (use a known GSTIN like this one). If blocked, fall back to Playwright on the web UI.
2. GeM bid listing: verify public URL is `https://bidplus.gem.gov.in/bidlists` and check if Cloudflare is active. If blocked, mark as stub.
3. EPFO unified portal: verify the public portal doesn't require login for establishment search. If login required, stub EPFO and document.
4. DGFT IEC register: confirm the public register URL is still active. If 404, try `https://www.dgft.gov.in/CP/` and navigate manually.

For each unknown: fetch the live page first, inspect HTML/JSON, then write the selector. Do NOT guess.
