# SOURCES.md

Every source has three properties:
- **Frequency**: How often the source actually changes
- **Cadence**: How often we pull
- **Events**: What constitutes a change worth firing

---

## MCA sources

### 1. MCA OGD (data.gov.in)
- **Gives**: 18L+ company master — CIN, name, status, capital, AGM dates, industry, state
- **Frequency**: Monthly
- **Cadence**: Monthly
- **Events**: Status changed. Capital moved >50%. AGM overdue >18mo on Active company. New company registered. Address changed.

### 2. MCA charge register
- **Gives**: Lender, amount, asset, charge creation/satisfaction
- **Frequency**: Per filing (continuous)
- **Cadence**: On enrichment trigger
- **Events**: New charge above ₹1Cr. Charge satisfied (positive). Multiple lenders on same CIN. Charge amount exceeds authorized capital.

### 3. MCA director/DIN data
- **Gives**: DIN-to-CIN mapping, appointments, cessations
- **Frequency**: Per filing (continuous)
- **Cadence**: Monthly via OGD + on enrichment trigger via CompData
- **Events**: Director resigned. New director appointed. CFO or auditor changed (high signal). Director now on >10 boards.

### 4. MCA CDM portal
- **Gives**: Government's own analytics on corporate sector — aggregates, trends, sector data
- **Frequency**: Quarterly
- **Cadence**: Quarterly
- **Events**: Sector-level stress patterns. Spike in strike-offs in specific geography. Nobody is using this source yet.

### 5. ROC filings (MCA21)
- **Gives**: Actual filed documents — AOC-4 (financials), MGT-7 (annual return), DIR-12, CHG-1
- **Frequency**: Per filing
- **Cadence**: On enrichment trigger only (some filings cost ₹100-500)
- **Events**: Revenue decline >20% YoY. Debt increase >50%. Auditor qualification. Going concern note.

---

## Legal sources

### 6. e-Courts (ecourts.gov.in)
- **Gives**: All court cases — civil, criminal, Section 138 cheque bounce
- **Frequency**: Daily
- **Cadence**: Weekly
- **Events**: New Section 138 case (cheque bounce — strongest early distress signal). Multiple 138 cases on same entity. New civil suit above threshold. Case disposed (resolution).
- **Scrape method**: By case type + date range. Entity resolve respondent names to CINs.

### 7. NCLT (nclt.gov.in)
- **Gives**: IBC proceedings — CIRP admissions, liquidation, resolution plans
- **Frequency**: Daily
- **Cadence**: Daily
- **Events**: New Section 7/9/10 filing. CIRP admitted. Moratorium declared. Resolution plan approved. Liquidation ordered. Timeline milestone crossed.

### 8. DRT (drt.gov.in)
- **Gives**: Debt recovery tribunal applications and orders
- **Frequency**: Daily
- **Cadence**: Daily
- **Events**: New recovery application. Order passed. Recovery amount above threshold.

### 9. SARFAESI notices (ibapi.in + bank portals)
- **Gives**: Section 13(2) demand notices, 13(4) possession notices, auction notices
- **Frequency**: Daily
- **Cadence**: Daily
- **Events**: New SARFAESI notice (bank declared NPA, 60-day window). Possession taken. Auction scheduled. Auction completed.
- **NOTE**: SARFAESI notice is CONFIRMED distress — bank has classified NPA. Precedes auction by weeks.

### 10. IBBI (ibbi.gov.in)
- **Gives**: Liquidation asset listings, resolution professional appointments, claims data
- **Frequency**: Weekly
- **Cadence**: Weekly
- **Events**: New liquidation asset listed. Asset sale notice published. Resolution professional appointed.

### 11. High Court commercial division
- **Gives**: Large commercial disputes not in DRT/NCLT
- **Frequency**: Daily
- **Cadence**: Weekly
- **Events**: New commercial suit above ₹1Cr. Interim order (injunction, attachment). Company named as defendant.

### 12. Supreme Court cause lists
- **Gives**: Largest corporate matters on appeal
- **Frequency**: Daily
- **Cadence**: Weekly
- **Events**: Corporate matter listed. Stay granted. Appeal dismissed.

### 13. Labour court orders
- **Gives**: Employee disputes, mass retrenchment notices, industrial dispute proceedings
- **Frequency**: Weekly
- **Cadence**: Monthly
- **Events**: Mass layoff notice filed. Industrial dispute raised. Back-wage order.

---

## Financial and trade sources

### 14. GST portal (gst.gov.in)
- **Gives**: GSTIN status — active, cancelled, suspended. Cancellation reason.
- **Frequency**: Continuous
- **Cadence**: Weekly (for monitored companies)
- **Events**: GST cancelled on Active MCA company (zombie signal). GST suspended (non-filing). New GST registration.
- **Requires**: CIN-to-GSTIN mapping (from AOC-4 or CompData)

### 15. DGFT (dgft.gov.in)
- **Gives**: IEC database, import/export code status
- **Frequency**: Continuous
- **Cadence**: Monthly
- **Events**: IEC cancelled (stopped trading). New IEC issued. Export obligation default.

### 16. SEBI bulk/block deals (BSE/NSE)
- **Gives**: Bulk deals >0.5% equity, block deals, insider trading disclosures
- **Frequency**: Daily
- **Cadence**: Daily
- **Events**: Promoter selling in bulk. Institutional investor exiting. Insider buying (confidence signal). Pledge creation/release.
- **NOTE**: Listed companies only, but cross-reference promoters to their private entities via director graph.

### 17. SEBI enforcement orders
- **Gives**: Regulatory actions — penalties, debarments, investigations
- **Frequency**: Weekly
- **Cadence**: Weekly
- **Events**: Company or promoter penalized. Trading restriction imposed. Investigation initiated.

### 18. CERSAI (cersai.org.in)
- **Gives**: All registered security interests — mortgages, hypothecation, pledges
- **Frequency**: Continuous
- **Cadence**: Monthly
- **Events**: New security interest above threshold. Multiple lenders. Interest satisfied.

### 19. RBI wilful defaulter list
- **Gives**: Quarterly published list of confirmed wilful defaulters
- **Frequency**: Quarterly
- **Cadence**: Quarterly
- **Events**: Company or director added to wilful defaulter list. CRITICAL signal — confirmed bad actor.

### 20. CCI filings (cci.gov.in)
- **Gives**: Merger/acquisition approvals, competition complaints
- **Frequency**: Monthly
- **Cadence**: Monthly
- **Events**: Merger/acquisition approved (major corporate move). Competition complaint filed. Penalty imposed.

### 21. State VAT/commercial tax portals
- **Gives**: Pre-GST era tax defaults still publicly listed
- **Frequency**: Archived/static
- **Cadence**: Once (backfill)
- **Events**: Historical tax default found for entity. Cross-reference with current signals for pattern.

---

## Government procurement sources

### 22. GeM (gem.gov.in)
- **Gives**: Government e-marketplace — seller registrations, orders won, order values
- **Frequency**: Daily
- **Cadence**: Weekly
- **Events**: Company won tender above ₹50L (revenue signal). High GeM activity (government revenue stream = stability). No wins despite registration (weak competitiveness).

### 23. CPPP (eprocure.gov.in)
- **Gives**: Central government tenders, awards, contractor details
- **Frequency**: Daily
- **Cadence**: Weekly
- **Events**: Tender awarded to monitored company. New sector entry via tender. Large contract above threshold.

---

## Workforce sources

### 24. EPFO (epfindia.gov.in)
- **Gives**: Establishment details, contribution data (indirect headcount)
- **Frequency**: Monthly
- **Cadence**: Monthly
- **Events (distress)**: Contribution drop >20% MoM (payroll missed). Establishment delisted.
- **Events (growth)**: Contribution surge >20% (hiring). New establishment registered.
- **Requires**: CIN-to-EPFO mapping (build passively from company websites, job postings, CompData)

### 25. ESIC (esic.gov.in)
- **Gives**: Employee state insurance establishment data, contribution status
- **Frequency**: Monthly
- **Cadence**: Monthly
- **Events**: Contribution default (payroll stress). New registration (hiring). Coverage expansion (growth).
- **Requires**: Same mapping challenge as EPFO

### 26. Naukri.com
- **Gives**: Active job listings per company, role types, posting dates
- **Frequency**: Daily
- **Cadence**: Weekly (for monitored companies)
- **Events**: Job count jumped >3x (hiring surge). All jobs removed (hiring freeze). Suddenly hiring lawyers or restructuring experts (trouble signal). Role type shift.
- **Method**: Firecrawl company job pages

### 27. Indeed India / Foundit
- **Gives**: Same as Naukri, different pool
- **Frequency**: Daily
- **Cadence**: Weekly
- **Events**: Same as Naukri
- **Method**: Firecrawl

### 28. Company career pages (targeted 500)
- **Gives**: Direct job postings from company websites
- **Frequency**: Varies
- **Cadence**: Weekly for top 500 interesting companies
- **Events**: Career page appeared (new hiring). Career page removed. Job count change.
- **Method**: Firecrawl on /careers URLs

### 29. Glassdoor India
- **Gives**: Employee reviews, ratings, CEO approval
- **Frequency**: Daily
- **Cadence**: Monthly
- **Events**: Review volume spike >3x (something happened internally). Rating dropped below 3.0. "Leaving because" review cluster.
- **Method**: Firecrawl company review pages

### 30. LinkedIn (indirect)
- **Gives**: Employee profile changes via Google index
- **Frequency**: Continuous
- **Cadence**: Monthly for flagged companies
- **Events**: Mass "formerly at [Company]" profile updates (internal exodus)
- **Method**: Google search via Firecrawl — `site:linkedin.com/in "formerly at [Company]"`

---

## Regulatory and sector-specific

### 31. Udyam registration portal
- **Gives**: MSME classification — micro/small/medium, NIC code, investment, turnover
- **Frequency**: Continuous
- **Cadence**: Quarterly
- **Events**: New registration. Classification change (micro→small = growing). Registration cancelled.

### 32. RERA (state portals)
- **Gives**: Real estate project registrations, developer compliance, complaints
- **Frequency**: Varies by state
- **Cadence**: Monthly
- **Events**: Multiple project delays by same developer. New registration. Complaint spike.

### 33. MOEF environment clearance portal
- **Gives**: Environmental clearances granted, denied, revoked for major projects
- **Frequency**: Weekly
- **Cadence**: Monthly
- **Events**: Clearance granted (major project greenlit). Clearance revoked (project halted). New application (signals planned expansion).

### 34. Pollution control boards (CPCB/SPCB)
- **Gives**: Consent to operate status, violations, closure orders
- **Frequency**: Varies by state
- **Cadence**: Quarterly
- **Events**: CTO revoked (factory shut down). Pollution violation notice. New CTO issued.

### 35. RBI NBFC/bank notifications
- **Gives**: License cancellations, enforcement actions, regulatory warnings
- **Frequency**: Weekly
- **Cadence**: Weekly
- **Events**: NBFC license cancelled. Enforcement action. Restrictions imposed.

---

## Signal combination matrix

Single signals are noise. Combinations are intelligence.

| Combination | Meaning | Severity |
|---|---|---|
| Filing decay + Sec 138 cases | Classic pre-insolvency pattern | RED |
| Filing decay + SARFAESI notice | Bank confirmed NPA, seizure imminent | CRITICAL |
| GST cancelled + MCA Active | Zombie — legally alive, operationally dead | RED |
| EPFO drop + hiring freeze + filing decay | Operational shutdown in progress | CRITICAL |
| Capital increase + hiring surge + GeM wins | Funded growth with government revenue | GREEN (strong) |
| Capital increase + director exits | Growth money but leadership unstable | AMBER |
| New registration + Udyam + GeM listing | New MSME entering government market | GREEN (opportunity) |
| DGFT active + GST active + EPFO growing | Healthy trading company | GREEN |
| Director from RED company joins new board | Contagion risk | WATCH |
| SEBI bulk deal (promoter selling) + SARFAESI | Promoter exiting, bank seizing | CRITICAL |
| CCI merger approval + capital increase | Major acquisition/expansion | GREEN (major move) |
| RBI wilful defaulter + active director elsewhere | Confirmed bad actor in other companies | RED (contagion) |
| MOEF clearance + CPPP tender win | Major infra project greenlighted | GREEN (opportunity) |
| Labour court mass layoff + EPFO drop | Confirmed workforce reduction | RED |
| Auditor changed + CFO resigned + filing delay | Governance collapse in progress | CRITICAL |

---

## CompData dependency mitigation

Primary enrichment: CompData API
Backup plan:
1. Tofler API (basic company data)
2. Direct MCA portal scraping (slow, free)
3. MCA21 AOC-4 XBRL parsing (financials directly from source)
4. Cached results compound — by Month 6, most frequent companies already cached
5. Alternative APIs: Signzy, Karza, Gridlines

Goal: self-sufficient on core fields (directors, basic financials, charges) by Phase 4.
