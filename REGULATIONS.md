# REGULATIONS.md

## Per-source legal basis

| # | Source | Legal basis | Can do | Cannot do |
|---|---|---|---|---|
| 1 | MCA OGD | NDSAP open license | Store, analyze, sell intelligence derived from it | Claim data is "MCA certified" |
| 2 | MCA charges | Public company filing | Display, analyze, compute signals | Access non-public filing sections |
| 3 | MCA directors | Published by MCA | Map networks, track movements | Sell director personal contact info |
| 4 | MCA CDM | Government analytics portal | Use aggregates and trends | Redistribute raw portal data |
| 5 | ROC filings | Public (some paid) | Parse XBRL, extract financials | Bypass MCA21 payment for paid filings |
| 6 | e-Courts | Public court records, RTI Act | Index cases, match to CINs | Access sealed cases, publish judge analytics |
| 7 | NCLT | Public cause lists/orders | Track, alert, analyze trends | Access CoC proceedings (not public) |
| 8 | DRT | Public orders | Index, alert, link to CINs | Nothing beyond public records |
| 9 | SARFAESI | Public auction notices | Track, alert, match to CINs | Misrepresent pending notice as confirmed seizure |
| 10 | IBBI | Public regulatory data | Track liquidations, asset listings | Nothing beyond public records |
| 11 | High Court | Public cause lists | Track commercial disputes | Access restricted/sealed matters |
| 12 | Supreme Court | Public cause lists | Track large corporate matters | Nothing beyond public records |
| 13 | Labour courts | Public orders | Track mass layoffs, disputes | Access individual employee records |
| 14 | GST portal | Public search | Check GSTIN status | Bulk-scrape beyond rate limits |
| 15 | DGFT | Public IEC database | Track import/export status | Access shipment-level detail (commercial) |
| 16 | SEBI deals | Published daily by exchanges | Track bulk/block deals, pledges | Nothing beyond published data |
| 17 | SEBI enforcement | Public orders | Track regulatory actions | Nothing beyond public records |
| 18 | CERSAI | Public search | Track security interests | Bulk-scrape beyond allowed access |
| 19 | RBI wilful defaulters | Published quarterly | Index, cross-reference, alert | Nothing beyond published list |
| 20 | CCI | Public filings/orders | Track mergers, competition actions | Access confidential filing details |
| 21 | State VAT | Historical public records | Cross-reference for patterns | Nothing beyond archived data |
| 22 | GeM | Public marketplace data | Track seller activity, tender wins | Access bid pricing or private seller data |
| 23 | CPPP | Public tenders | Track awards, contractor info | Access sealed bid documents |
| 24 | EPFO | Public establishment search | Track contribution patterns | Access individual employee records |
| 25 | ESIC | Public establishment data | Track contribution status | Access individual employee records |
| 26 | Naukri | Public company job pages | Track job counts, role types | Scrape resumes, recruiter contacts |
| 27 | Indeed/Foundit | Public company job pages | Same as Naukri | Same as Naukri |
| 28 | Career pages | Public company websites | Track hiring activity | Violate robots.txt |
| 29 | Glassdoor | Public review pages | Track review counts, ratings | Reproduce full review text (copyright) |
| 30 | LinkedIn indirect | Google-indexed public profiles | Detect mass departures | Scrape LinkedIn directly |
| 31 | Udyam | Public registration portal | Track MSME classifications | Nothing beyond public search |
| 32 | RERA | State portal public data | Track developer compliance | Access beyond what portal publishes |
| 33 | MOEF | Public clearance portal | Track project approvals/denials | Nothing beyond public records |
| 34 | CPCB/SPCB | Public violation data | Track CTO status, violations | Access beyond published data |
| 35 | RBI notifications | Public press releases | Track NBFC actions | Nothing beyond public records |

## Key legal nuances

**Section 138 cases**: Criminal cases. Respondent is accused, not convicted. System says "X active cheque bounce cases filed against" — never "company bounced X cheques." The distinction matters in court.

**SARFAESI notices**: A Section 13(2) notice means bank classified account as NPA. It is NOT seizure or auction — those come later. System must clearly label the stage: "demand notice issued" vs "possession taken" vs "auction scheduled."

**Health scores**: Never called credit scores. Never positioned as credit rating. Always labeled as "computed from public data." Always accompanied by disclaimer.

**Director data**: Names and DINs published by MCA are public. Mapping directorships across companies is public information analysis. Selling director personal contact info (phone, personal email, home address) crosses into personal data territory — don't.

**SEBI data to private company mapping**: Promoter sells shares in listed Company A. We cross-reference that promoter (via DIN) to their private Company B and fire a contagion alert. This is legal — all data points are public. But the intelligence connection is proprietary and valuable.

## Play-specific regulatory requirements

**Play 3 (Corporate Action Router):**
- No financial license needed — NBFC takes the regulated action, we provide the trigger signal
- E&O insurance mandatory before live deployment
- Indemnification clause in every enterprise contract
- Human-in-the-loop gate for actions above defined threshold
- Full audit trail on every trigger event

**Play 1 (Trade Credit Underwriting):**
- Needs IRDAI-licensed insurance partner to issue actual policy
- We provide underwriting intelligence, partner carries the risk
- GIFT City IFSCA sandbox available for testing
- Cannot issue insurance or guarantee coverage ourselves

**Play 2 (Algorithmic Distressed Asset Acquisition):**
- Needs capital partner for actual bidding
- IBC Section 29A eligibility check on bidder (not us — the capital partner)
- We provide intelligence and bid analysis, partner makes legal filing
- Human must press submit on any legal filing — cannot fully automate

## GST

- 18% on information technology services (SAC 998314)
- Registration mandatory when revenue > ₹20L/year
- B2B invoicing with subscriber GSTIN
