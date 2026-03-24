# COMPLIANCE.md

## What we are legally

A data intelligence tool that aggregates public government data and computes signals. We are NOT a credit bureau (CICRA), NOT an NBFC (RBI), NOT a credit rating agency (SEBI), NOT a KYC service provider. This distinction is critical and must never blur.

## Key regulations

**IT Act, 2000**: Reasonable security for data processing. We handle primarily corporate data. Subscriber data (email, watchlists) gets standard protection.

**DPDPA, 2023**: Company data (CIN, financials, filings) is not personal data. Director names/DINs are publicly published by MCA — legitimate use exemption. We do not sell director personal information.

**PMLA / RBI KYC**: Does not apply to us directly. We are a data provider, not a regulated entity. Subscribers use our intelligence in their own compliance processes. We do not perform KYC on their behalf.

**Companies Act, 2013 / NDSAP**: OGD data is published under open license for commercial use. Direct MCA portal scraping: respect ToS, prefer OGD and licensed APIs.

**CICRA**: We are NOT a credit information company. Our health scores are based on public data, not credit bureau data. Never call them "credit scores." Never position as credit bureau substitute.

## The false positive problem — CRITICAL

When the engine triggers real-world actions (Play 3), a false positive is a lawsuit, not a complaint.

**Scenario**: System flags Company X as RED. NBFC auto-freezes credit line. Company X was actually healthy — system misread a court name match. Company X sues.

**Required protections before Play 3 goes live**:

1. **Errors & omissions (E&O) insurance** — professional liability coverage for incorrect intelligence that causes financial harm
2. **Indemnification clause** in every enterprise contract — subscriber assumes responsibility for actions taken on intelligence
3. **Confidence thresholds** — automated actions only fire above 95% confidence, computed from 3+ independent confirming signals
4. **Human-in-the-loop gate** — for CRITICAL severity events that trigger real-world actions, require human confirmation before execution
5. **Audit trail** — every event detection, every score change, every alert delivery logged with timestamps and source data. Defensible in court.
6. **Published accuracy rate** — track prediction accuracy monthly. "Our RED alerts correlated with actual distress 87% of the time." Transparent, not claimed.

## Data freshness SLA

Every source has documented latency. If our signal fires 45 days late, it's useless.

| Source | Actual frequency | Our cadence | Max latency |
|---|---|---|---|
| OGD CSV | Monthly | Monthly | 30 days (weakest link) |
| NCLT cause lists | Daily | Daily | 24 hours |
| DRT orders | Daily | Daily | 24 hours |
| SARFAESI notices | Daily | Daily | 24 hours |
| e-Courts new cases | Daily | Weekly | 7 days |
| SEBI bulk deals | Daily | Daily | 24 hours |
| RBI wilful defaulters | Quarterly | Quarterly | 90 days |
| GST status | Continuous | Weekly | 7 days |
| EPFO/ESIC | Monthly | Monthly | 30 days |
| Job portals | Daily | Weekly | 7 days |
| GeM/CPPP | Daily | Weekly | 7 days |

Must publish this table. Subscribers need to know how current each signal is.

## Disclaimers

**On all intelligence output**:
"Derived from publicly available government data. Not a credit score, credit rating, or regulatory compliance certificate. Verify independently before acting. Data latency varies by source — see published SLA."

**On health scores**:
"Computed from public filings, court records, and governance data. Not a credit assessment. Indicative only."

**On automated action triggers (Play 3)**:
"Signals are intelligence inputs, not instructions. The subscribing institution bears full responsibility for any action taken. Signals carry documented confidence levels and source latency."

## GIFT City IFSCA sandbox

Path for Play 1 (trade credit underwriting). IFSCA sandbox allows testing financial products with lighter regulation before full IRDAI licensing. Relevant when the engine is proven accurate and a licensed insurance partner is identified.

## Registrations needed

- **Now**: GST (if revenue > ₹20L). MSME/Udyam (credibility).
- **Later**: Company incorporation when scaling. E&O insurance when Play 3 approaches.
- **Never (unless scope changes)**: CICRA, NBFC, credit rating agency.

## Annual review

- [ ] Update source ToS compliance
- [ ] Review accuracy metrics
- [ ] Update data freshness SLA
- [ ] Review E&O insurance adequacy
- [ ] Check for new regulations affecting classification
- [ ] Audit subscriber contracts for indemnification
