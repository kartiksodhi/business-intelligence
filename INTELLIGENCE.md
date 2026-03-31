# INTELLIGENCE.md

## What this is
Strict operational boundaries for all LLM interactions and mathematical formulas for the backend scoring engine. Acts as the ultimate constraint layer to prevent token bloat, database insertion failures, and AI hallucinations.

## CRITICAL IMPLEMENTATION ORDER — READ FIRST
- **Part 1 — Phase 1. Implement before any LLM call is ever made.** If strict JSON constraints are not in place from day one, malformed strings enter PostgreSQL immediately and corrupt the data foundation. Retroactive cleaning is not feasible for a solo operator. The first entity resolution LLM call happens in Phase 1 — the constraint must be live before that call is written.
- **Part 2 — Phase 2. Implement when legal document scrapers are built.** SARFAESI and NCLT scrapers start in Phase 2. VLM routing must be configured at the same time those scrapers are written. If pytesseract attempts to parse legal notices and fails silently, legal_events table fills with empty fields before anyone notices.
- **Part 3 — Phase 3+ only. Implement when data is proven and stable.** Mathematical models require real accumulated outcome data. Do not implement before Phase 3.

## Agent Responsibilities
- **Claude Code:** Enforce Part 1 prompt constraints before any LLM call is written. Route SARFAESI/NCLT documents to VLM in Phase 2, strictly bypassing pytesseract for legal documents.
- **Codex:** Build Pydantic schemas for all LLM response endpoints. Translate Part 3 mathematical formulas into Python/SQL backend jobs in Phase 3.

---

## PART 1: Strict LLM Boundaries — IMPLEMENT IN PHASE 1

LLMs must NEVER output conversational text in the backend pipeline. All internal AI calls must enforce strict JSON schemas to ensure flawless PostgreSQL ingestion.

### 1. Entity Resolution Fallback (LLM Queue)
- **Usage:** Triggered ONLY when pg_trgm and Jaro-Winkler return 50-70% confidence with multiple candidates.
- **Model:** claude-sonnet-4-6
- **System Prompt:**
  > You are a deterministic database routing function. You do not converse. Compare the provided unstructured 'Target Name' against the 'Candidate Array' of legal CINs and Names.
  > Output strictly as a JSON object: {"matched_cin": "<CIN_STRING_OR_NULL>", "confidence_score": <INTEGER_1_100>, "reasoning_flag": "<SHORT_STRING>"}.
  > If no logical match exists, return null for matched_cin. Do not output markdown or backticks.

### 2. Alert Analyst Synthesizer (Zero-Hallucination)
- **Usage:** ONLY at moment of delivery for CRITICAL and ALERT severity batches to human subscribers.
- **Model:** claude-sonnet-4-6
- **System Prompt:**
  > You are a quantitative distressed-asset analyst. You are provided with a JSON array of verified events for a Corporate Identification Number (CIN).
  > Rule 1: Output exactly three concise bullet points.
  > Rule 2: State the facts, the immediate financial implication, and the contagion risk based ONLY on the provided JSON.
  > Rule 3: STRICT PROHIBITION: Do not introduce outside knowledge, assume outcomes, or use emotional adjectives (e.g., "massive", "terrible", "doomed"). Keep the tone clinical, objective, and financially rigorous.

### 3. Pydantic Validation Wrapper — mandatory on every LLM call
Codex must wrap every LLM call in a Pydantic validator. If response fails schema validation — log raw response, retry once with temperature=0. If second attempt also fails — route to entity_resolution_queue as UNRESOLVABLE. Never pass a failed LLM response downstream.

```python
from pydantic import BaseModel, ValidationError
from typing import Optional

class EntityResolutionResponse(BaseModel):
    matched_cin: Optional[str]
    confidence_score: int
    reasoning_flag: str

def call_llm_resolver(target_name, candidates):
    raw = claude_api_call(prompt, model="claude-sonnet-4-6")
    try:
        return EntityResolutionResponse.model_validate_json(raw)
    except ValidationError:
        raw = claude_api_call(prompt, model="claude-sonnet-4-6", temperature=0)
        try:
            return EntityResolutionResponse.model_validate_json(raw)
        except ValidationError:
            log_failed_llm_response(raw)
            route_to_unresolvable_queue(target_name)
            return None
```

---

## PART 2: Vision & OCR Routing — IMPLEMENT IN PHASE 2 WITH LEGAL SCRAPERS

pytesseract is retained exclusively for basic CAPTCHAs. Strictly prohibited for unstructured legal document extraction due to high failure rates on Indian stamps, signatures, and poor scans.

### 1. Routing decision logic
```python
def route_document(source, file):
    if source in ['sarfaesi', 'nclt', 'drt']:
        return vlm_extract(file)       # VLM — structured legal extraction
    elif source == 'captcha':
        return pytesseract_solve(file) # pytesseract — CAPTCHAs only
    else:
        return playwright_scrape(file) # Playwright — everything else
```

### 2. SARFAESI, DRT & NCLT VLM Prompt
- **Target sources:** Bank seizure notices (Section 13(2), 13(4)), auction notices, NCLT resolution plans, DRT orders.
- **Model:** claude-sonnet-4-6 (vision)
- **System Prompt:**
  > You are a financial data extraction engine processing an Indian legal/bank notice.
  > Task: Extract the Borrower Name, Lender (Bank), Demand Amount, and Date.
  > Constraint 1: Mathematically convert "Lakhs" or "Crores" into a plain integer (e.g., "1.5 Crore" = 15000000).
  > Constraint 2: Ignore handwritten "Received" stamps; prioritize the printed letterhead.
  > Output strictly as JSON: {"demand_amount_inr": <INT_OR_NULL>, "date_of_notice": "<YYYY-MM-DD_OR_NULL>", "lender_name": "<STRING_OR_NULL>", "borrower_cin": "<STRING_OR_NULL>"}

---

## PART 3: Quantitative Alpha Models — PHASE 3+ ONLY

Backend Python detection engine only. LLMs do not calculate these. Codex implements as pure Python/SQL jobs.

### 1. Poisson Process for Legal Velocity (e-Courts/DRT)
Replaces flat case-count thresholds with probability-based anomaly detection.
- **Formula:** P(k) = (λ^k × e^(-λ)) / k!
  Where k = cases observed in 30-day window, λ = historical monthly baseline rate for that company.
- **Execution:** If P(k) < 0.05 — statistically significant spike — automatically upgrade event severity to CRITICAL, bypass standard batch wait times.

### 2. Shadow Working Capital Estimation (MSME Proxy)
Real-time cash flow proxy when AOC-4 financials are delayed or unavailable.
- **Formula:** ΔWC_proxy = α(ΔEPFO_headcount) - β(ΔT_GST_delay)
  Where α = sector-specific baseline wage multiplier, β = penalty weight for GST filing delay in days.
- **Execution:** Negative ΔWC_proxy + static or dropping EPFO headcount = WATCH alert "Imminent Cash Crunch."

### 3. Eigenvector Centrality for Contagion (Network Risk)
Upgrades static contagion penalty (-15/-5) into dynamic capital-weighted network risk score.
- **Formula:** x_v = (1/λ) × Σ x_t for all t in N(v)
  Where x_v = contagion risk of company v, N(v) = all companies sharing a director with v.
- **Execution:** Weight governance_graph connections by authorized capital. Large enterprise defaulting cascades mathematically higher penalty to connected shell companies than vice versa.

### 4. Deterministic Shell Company Detection
Fires SHELL_RISK (WATCH severity) when all conditions met simultaneously. No LLM or external API required.
- **Formula (Boolean Logic):**
  IF (Age_Months < 36) AND (Auth_Capital <= 1000000) AND (AGM_Date IS NULL) AND (EPFO_Active == False) AND (GSTIN_Active == False) AND (Max_Director_Board_Count >= 5) THEN Trigger = TRUE
- **Execution:** Runs monthly after OGD diff.

### 5. Sector Cluster Anomaly (Geographic Stress)
Detects macro-economic sector collapse before it hits the news.
- **Formula:** Sector Stress Index = Σ(Score_baseline - Score_current) / N_cluster
  Applied across all companies sharing same NIC code and State.
- **Execution:** If 5+ companies in same state/sector cross into AMBER/RED within 30-day rolling window — fire SECTOR_CLUSTER_ALERT to all relevant watchlists immediately.

---

## PART 4: Product unbundling thesis

ICIE (full platform) is hard to sell and hard to explain. Unbundle into vertical products per buyer. Each is a standalone product with its own buyer. All use ICIE as backend.

| Product | Buyer | Sales motion |
|---|---|---|
| Director Contagion Tracker | NBFCs, banks | 20-min sales call, immediate value |
| Builder Creditworthiness Score | Construction finance NBFCs | RERA data → credit risk |
| Court Judgment Intelligence | Lenders, PE, advocates | eCourts + NCLT feed |
| GeM Procurement Pattern Engine | MSMEs, PE sector research | Tender wins + vendor patterns |

### Additional unbundled data plays
- **GeM portal** → procurement winners, contract values, vendor patterns → buyers: PE, banks, MSMEs
- **NDAP (NITI Aayog)** → national datasets across all sectors → free download, no scraping
- **Parliamentary debates** → policy shift detection → buyers: hedge funds, corporates
- **MCA charge registry** → bank exposure per company → lenders would pay
- **RBI enforcement actions** → NBFC/bank flags → compliance teams
- **CPPP** → infra tenders, L1 margins, contractor patterns → construction finance
- **RERA** → builder risk → NBFCs, homebuyers, RE funds
