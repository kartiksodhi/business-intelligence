# PHASE3_BATCH2_SPEC.md

## What this is
Scraper specs for Phase 3 Batch 2 — 14 government sources + operator approval decision point for 5 Firecrawl sources.
Build after Batch 1 is passing.

## Hard constraints (from CLAUDE.md)
- Playwright before Firecrawl. Always.
- Firecrawl sources (Naukri, Glassdoor, Indeed, career pages, LinkedIn) are BLOCKED until operator gives explicit approval.
- Scrape by recency not entity.
- Diff not reprocess.

## Build order
1. ingestion/scrapers/mca_charges.py
2. ingestion/scrapers/mca_directors.py
3. ingestion/scrapers/high_court.py
4. ingestion/scrapers/supreme_court.py
5. ingestion/scrapers/labour_court.py
6. ingestion/scrapers/sebi_bulk_deals.py
7. ingestion/scrapers/cersai.py
8. ingestion/scrapers/cci.py
9. ingestion/scrapers/rbi_wilful_defaulter_directors.py  (director cross-ref — extends Batch 1 source)
10. ingestion/scrapers/esic.py
11. ingestion/scrapers/rera.py
12. ingestion/scrapers/moef.py
13. ingestion/scrapers/cpcb.py
14. ingestion/scrapers/state_vat.py  (static backfill — one-time)
15. detection/sector_cluster.py
16. Tests for each

---

## Source 1: MCA Charge Register (Source #2)

**URL**: MCA21 portal — https://www.mca.gov.in/content/mca/global/en/mca/fo-llp-filing/charge.html
**Cadence**: On enrichment trigger only (when a CIN is flagged AMBER or RED)
**Scrape strategy**:
- Not a bulk scrape. Triggered per-CIN when health score crosses threshold.
- Playwright: navigate to MCA charge search, enter CIN, extract charge table.
- Extract: charge_id, creation_date, satisfaction_date, lender_name, charge_amount_inr, asset_description, status (Open/Satisfied)
- Hash per charge_id. Fire only on new charge_ids or status changes.
- Entity resolve lender_name to known bank/FI list (simple string match — no LLM needed, finite list).

**Events fired**:
- `CHARGE_CREATED`: New charge above ₹1Cr → RED
- `CHARGE_SATISFIED`: Existing charge marked satisfied → GREEN
- `CHARGE_EXCEEDS_CAPITAL`: charge_amount_inr > authorized_capital → CRITICAL
- `MULTIPLE_LENDERS`: 3+ open charges on same CIN within 12 months → RED

**source_state key**: `mca_charge:{cin}:last_checked`
**entity resolution**: CIN already known. Trigger is per-CIN. No resolution needed.
**CAPTCHA**: MCA21 has CAPTCHAs. pytesseract.

---

## Source 2: MCA Director/DIN Data (Source #3)

**URL**: MCA OGD monthly CSV also includes director data. Supplemented by MCA21 DIN search.
**Cadence**: Monthly via OGD diff + on enrichment trigger
**Scrape strategy**:
- Primary: parse director columns from OGD CSV during monthly OGD diff run (already happening).
- Supplementary: when a CRITICAL event fires, trigger per-CIN director refresh via MCA21.
- Extract: din, director_name, designation (Director/CFO/Auditor/CS), appointment_date, cessation_date
- Update director_graph table. Compute board_count per DIN (how many active boards).
- Hash by (din + cin + designation + cessation_date). Fire on change.

**Events fired**:
- `DIRECTOR_RESIGNED`: cessation_date populated → AMBER
- `CFO_RESIGNED`: designation=CFO + cessation → RED (high signal)
- `AUDITOR_CHANGED`: designation=Auditor changes → RED
- `DIRECTOR_OVERLOADED`: director now on >10 active boards → AMBER
- `CONTAGION_DIRECTOR`: director from a RED/CRITICAL company joins this board → RED

**source_state key**: `mca_director:{cin}:last_refresh`
**entity resolution**: DIN→name known. Cross-reference director_graph for contagion check.

---

## Source 3: High Court Commercial Division (Source #11)

**URL**: eCourts API — https://services.ecourts.gov.in/ecourtindia_v6/
High Courts use the same eCourts infrastructure as district courts.
**Cadence**: Weekly
**Scrape strategy**:
- Playwright: same approach as e-Courts scraper (Batch 1 Phase 2).
- Filter: case_type = "Commercial" OR "CS" (Commercial Suit), date_filed = last 7 days.
- Extract: case_number, filing_date, petitioner_name, respondent_name, court_name, claim_amount_inr
- Entity resolve respondent_name to CIN. Petitioner rarely a company of interest.
- Hash by case_number. Fire only new hashes.
- Only fire if claim_amount_inr > 10000000 (₹1Cr threshold).

**Events fired**:
- `HIGH_COURT_COMMERCIAL_SUIT`: New commercial suit above ₹1Cr, company as respondent → RED
- `HIGH_COURT_INJUNCTION`: Interim order / injunction granted → RED
- `HIGH_COURT_ATTACHMENT`: Asset attachment ordered → CRITICAL

**source_state key**: `high_court:last_filed_date`
**entity resolution**: EntityResolver on respondent_name.

---

## Source 4: Supreme Court Cause Lists (Source #12)

**URL**: https://main.sci.gov.in/case-status (cause list search)
Daily cause list PDF: https://main.sci.gov.in/php/cl_next.php
**Cadence**: Weekly
**Scrape strategy**:
- Download the weekly cause list PDF/HTML.
- Parse party names from listed matters.
- Filter: only matters where a company name (contains "Ltd", "Pvt", "LLP", "Corp") appears as petitioner or respondent.
- Entity resolve company names to CINs.
- Hash by (matter_number + date). Fire only new hashes.

**Events fired**:
- `SC_MATTER_LISTED`: Corporate matter listed in Supreme Court → AMBER
- `SC_STAY_GRANTED`: Stay order granted → RED
- `SC_APPEAL_DISMISSED`: Appeal dismissed (adverse) → RED

**source_state key**: `supreme_court:last_cause_list_date`
**entity resolution**: EntityResolver on party names. Low resolution rate expected — SC matters use formal legal names.

**CAPTCHA**: SCI site has no CAPTCHA but has rate limiting. 3s delay between requests.

---

## Source 5: Labour Court Orders (Source #13)

**URL**: State-specific. Start with Maharashtra and Delhi only (highest volume).
- Maharashtra: https://lci.gov.in/ (Labour Court India portal)
- Delhi: https://delhilabourcourt.nic.in/
**Cadence**: Monthly
**Scrape strategy**:
- Playwright: load recent orders page, filter by date = last 30 days.
- Extract: case_number, order_date, establishment_name, order_type (retrenchment/back-wages/ID-Act), employee_count_affected
- Entity resolve establishment_name to CIN.
- Hash by case_number. Fire only new hashes.

**Events fired**:
- `LABOUR_MASS_RETRENCHMENT`: Retrenchment order, employee_count_affected > 50 → RED
- `LABOUR_BACK_WAGES`: Back-wage order passed → AMBER
- `LABOUR_INDUSTRIAL_DISPUTE`: New industrial dispute registered → AMBER

**source_state key**: `labour_court:{state}:last_order_date`
**entity resolution**: EntityResolver on establishment_name. Labour court names are informal — expect lower resolution rate. Unresolved → unmapped_signals.

**Stub condition**: If portal is 404 or structure unclear — stub and log. Do not fail scheduler.

---

## Source 6: SEBI Bulk/Block Deals (Source #16)

**URL**: BSE bulk deals: https://www.bseindia.com/data/xml/notices.xml (or bulk deal CSV)
NSE bulk deals: https://www.nseindia.com/api/bulk-deal-data
**Cadence**: Daily
**Scrape strategy**:
- Download daily bulk deal CSV from BSE and NSE.
- Filter: client_name contains promoter names OR quantity > 0.5% of total shares.
- Extract: deal_date, scrip_code, company_name, client_name, deal_type (Buy/Sell), quantity, price, deal_value_inr
- Entity resolve company_name to CIN. Only listed companies — use ISIN if available.
- Hash by (deal_date + scrip_code + client_name). Fire only new hashes.
- Identify promoter sells: cross-reference client_name against director_graph for that company.

**Events fired**:
- `SEBI_BULK_DEAL_PROMOTER_SELL`: Promoter selling >0.5% equity → RED
- `SEBI_BULK_DEAL_INSTITUTIONAL_EXIT`: Institutional investor exiting → AMBER
- `SEBI_INSIDER_BUY`: Insider buying (confidence signal) → GREEN
- `SEBI_PLEDGE_CREATED`: Promoter pledge disclosed → AMBER

**source_state key**: `sebi_bulk_deals:last_deal_date`
**entity resolution**: ISIN lookup first (fastest for listed companies). Fall back to EntityResolver on company_name.

---

## Source 7: CERSAI (Source #18)

**URL**: https://www.cersai.org.in/CERSAI/home.prg
Public search: https://www.cersai.org.in/CERSAI/securedAsset.prg
**Cadence**: Monthly
**Scrape strategy**:
- CERSAI has a public search by borrower name / CIN.
- For monitored companies (AMBER/RED health band only — not all 18L), run CIN search.
- Extract: si_id, creation_date, satisfaction_date, secured_creditor_name, asset_description, amount_inr
- Hash by si_id. Fire on new security interests only.

**Events fired**:
- `CERSAI_NEW_SI`: New security interest above ₹1Cr → RED
- `CERSAI_MULTIPLE_LENDERS`: 3+ open SIs on same CIN → RED
- `CERSAI_SI_SATISFIED`: Security interest satisfied → GREEN

**source_state key**: `cersai:{cin}:last_checked`
**entity resolution**: CIN known from trigger. No resolution needed.
**CAPTCHA**: CERSAI has CAPTCHA. pytesseract.

---

## Source 8: CCI Filings (Source #20)

**URL**: https://www.cci.gov.in/merger-and-acquisitions/orders/summary
CCI orders: https://www.cci.gov.in/antitrust/orders/summary
**Cadence**: Monthly
**Scrape strategy**:
- Playwright: load CCI orders/M&A page, filter by date = last 30 days.
- Extract: order_date, case_number, party_names, order_type (Approval/Penalty/Complaint), deal_value_inr
- Entity resolve party_names to CINs.
- Hash by case_number. Fire only new hashes.

**Events fired**:
- `CCI_MERGER_APPROVED`: M&A approval → GREEN (major corporate move)
- `CCI_PENALTY`: Competition penalty imposed → RED
- `CCI_COMPLAINT`: Competition complaint filed → AMBER

**source_state key**: `cci:last_order_date`
**entity resolution**: EntityResolver on party_names. CCI uses formal legal names — good resolution expected.

---

## Source 9: ESIC (Source #25)

**URL**: https://www.esic.gov.in/establishmentsearch
**Cadence**: Monthly
**Scrape strategy**:
- Same approach as EPFO scraper (Batch 1).
- Prerequisite: CIN→ESIC establishment code mapping from master_entities.esic_id.
- Extract: establishment_name, esic_code, status (Covered/Cancelled), last_contribution_month
- Hash by (esic_code + status). Fire on status change.

**Events fired**:
- `ESIC_DEFAULT`: Contribution default → AMBER
- `ESIC_CANCELLED`: Establishment cancelled → RED
- `ESIC_NEW`: New ESIC registration → GREEN

**source_state key**: `esic:{esic_id}`
**entity resolution**: CIN known from master_entities join.

---

## Source 10: RERA (Source #32)

**URL**: State-specific. Start with MahaRERA and RERA Rajasthan only.
- MahaRERA: https://maharera.mahaonline.gov.in/
- RERA Rajasthan: https://rera.rajasthan.gov.in/
**Cadence**: Monthly
**Scrape strategy**:
- Playwright: search by promoter name. Filter for recently updated projects (last 30 days).
- Extract: project_name, promoter_name, registration_date, status (Registered/Lapsed/Revoked), complaints_count
- Entity resolve promoter_name to CIN.
- Hash by (project_id + status + complaints_count). Fire on change.

**Events fired**:
- `RERA_LAPSED`: Project registration lapsed → RED
- `RERA_REVOKED`: Registration revoked → CRITICAL
- `RERA_COMPLAINT_SPIKE`: complaints_count jumped >3x in 30 days → RED
- `RERA_NEW_PROJECT`: New project registered → GREEN

**source_state key**: `rera:{state}:{project_id}`
**entity resolution**: EntityResolver on promoter_name.

**Stub condition**: State RERA portals are inconsistent. If portal unavailable → stub, do not fail.

---

## Source 11: MOEF Environment Clearance (Source #33)

**URL**: https://parivesh.nic.in/ (PARIVESH portal)
**Cadence**: Monthly
**Scrape strategy**:
- Playwright: load PARIVESH, filter proposals by status_change_date = last 30 days.
- Extract: proposal_no, project_name, proponent_name, clearance_type, status (Granted/Refused/Revoked), project_cost_inr
- Entity resolve proponent_name to CIN.
- Hash by (proposal_no + status). Fire on status change.

**Events fired**:
- `EC_GRANTED`: Environmental clearance granted → GREEN (major project greenlit)
- `EC_REFUSED`: Clearance refused → RED
- `EC_REVOKED`: Clearance revoked → CRITICAL (project halted)
- `EC_NEW_APPLICATION`: Large application (project_cost > ₹100Cr) → GREEN (signals planned expansion)

**source_state key**: `moef:{proposal_no}`
**entity resolution**: EntityResolver on proponent_name.

---

## Source 12: Pollution Control / CPCB (Source #34)

**URL**: https://www.cpcb.gov.in/ (CPCB) — state boards vary
Start with CPCB national portal only in Batch 2. State boards in Batch 3.
**Cadence**: Quarterly
**Scrape strategy**:
- CPCB publishes enforcement notices and CTO (Consent to Operate) revocations on its website.
- Playwright: load enforcement/closure orders page, filter by date = last 90 days.
- Extract: notice_date, unit_name, unit_address, violation_type, action_taken (CTO revoked/penalty/closure)
- Entity resolve unit_name to CIN.

**Events fired**:
- `CTO_REVOKED`: Consent to Operate revoked → CRITICAL (factory/plant shut down)
- `POLLUTION_NOTICE`: Violation notice issued → AMBER
- `CPCB_CLOSURE_ORDER`: Closure order → CRITICAL

**source_state key**: `cpcb:last_notice_date`
**entity resolution**: EntityResolver on unit_name. Industrial unit names are informal — expect lower resolution. Unresolved → unmapped_signals.

---

## Source 13: State VAT / Commercial Tax (Source #21)

**Type**: Static backfill — one-time scrape, not recurring.
**URL**: State-specific. Start with Maharashtra and Gujarat.
- Maharashtra: https://mahavat.gov.in/Mahavat/defaulters.jsp (if still live)
- Gujarat: https://vat.gujarat.gov.in/
**Cadence**: Once. Tag as `backfill` in source column.
**Scrape strategy**:
- Playwright: download the defaulter list. Parse names and default amounts.
- Entity resolve names to CINs.
- Insert into legal_events with event_type = `VAT_HISTORICAL_DEFAULT`, source = `state_vat_backfill`.
- Do not re-run. Mark in source_state as completed.

**Events fired**:
- `VAT_HISTORICAL_DEFAULT`: Historical tax default found → WATCH (context signal, not actionable alone)

**source_state key**: `state_vat:{state}:backfill_complete`

---

## Detection Module: Sector Cluster Anomaly

**File**: detection/sector_cluster.py
**Trigger**: Monthly, runs after health scores recompute.
**Formula** (from INTELLIGENCE.md Part 3 §5):
```
Sector Stress Index = Σ(Score_baseline - Score_current) / N_cluster
Applied across all companies sharing same NIC code AND registered_state.
```

**SQL**:
```sql
WITH cluster AS (
    SELECT
        me.cin,
        me.nic_code,
        me.registered_state,
        hs.health_score AS current_score,
        hs.baseline_score,
        (hs.baseline_score - hs.health_score) AS score_drop
    FROM master_entities me
    JOIN health_scores hs ON hs.cin = me.cin
    WHERE hs.band IN ('AMBER', 'RED')
      AND hs.last_computed > NOW() - INTERVAL '30 days'
),
cluster_stats AS (
    SELECT
        nic_code,
        registered_state,
        COUNT(*) AS n_distressed,
        AVG(score_drop) AS avg_drop
    FROM cluster
    GROUP BY nic_code, registered_state
    HAVING COUNT(*) >= 5
)
SELECT * FROM cluster_stats
WHERE avg_drop > 15
```

**Event fired**: `SECTOR_CLUSTER_ALERT` at CRITICAL severity.
- Fire for each company in the cluster (so their watchlist subscribers get notified).
- notes field: `{"cluster_size": N, "nic_code": "...", "state": "...", "avg_score_drop": X}`
- Deduplicate: do not re-fire same (nic_code + state) cluster within 30 days.

---

## Firecrawl sources — BLOCKED pending operator approval

These 5 sources require Firecrawl (paid per crawl). Do NOT implement until operator explicitly approves.

| Source | Why Firecrawl | Monthly cost estimate |
|---|---|---|
| Naukri.com | Anti-bot, JS-heavy | ~₹500-2000 depending on volume |
| Indeed India / Foundit | Same | ~₹500-1500 |
| Company career pages (500) | Mixed — some need it | ~₹1000-3000 |
| Glassdoor India | Anti-bot, login wall | ~₹500-1000 |
| LinkedIn (indirect via Google) | Google search via Firecrawl | ~₹200-500 |

**Total if all approved**: ~₹2700-8000/month additional.
**Operator action required**: Explicit go/no-go before Codex touches these.

---

## Scheduler additions

Add to ingestion/scheduler.py:
- mca_charges: on-demand (called by routing layer when CIN crosses AMBER) — not scheduled
- mca_directors: monthly (2nd of month, 6am)
- high_court: weekly (Friday 6am)
- supreme_court: weekly (Friday 7am)
- labour_court: monthly (10th of month, 8am)
- sebi_bulk_deals: daily (5pm — after market close)
- cersai: monthly (15th of month, 9am) — AMBER/RED companies only
- cci: monthly (20th of month, 8am)
- esic: monthly (3rd of month, 7am)
- rera: monthly (12th of month, 8am)
- moef: monthly (8th of month, 8am)
- cpcb: quarterly (15th of Jan/Apr/Jul/Oct, 9am)
- state_vat: one-time on first run, then never again
- sector_cluster: monthly (last day of month, 11:30pm — after shell_detector)

---

## Pass criteria

- pytest tests/ — all previous tests pass + new tests pass
- python ingestion/scheduler.py starts without error
- sector_cluster.py runs against bi_engine database without error
- mca_charges scraper accepts a CIN parameter and returns charge list
- state_vat scraper marks backfill_complete in source_state after one run and skips on subsequent runs
- Firecrawl sources: zero code written. Only a stub file with a NotImplementedError and a comment: "Awaiting operator approval per CLAUDE.md"

DATABASE_URL=postgresql://localhost/bi_engine
