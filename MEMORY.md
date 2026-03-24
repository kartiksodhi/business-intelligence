# MEMORY.md

## What was decided

### Architecture: event-driven, not query-driven
Nobody searches. The system watches 35+ sources, detects changes, scores them, routes alerts. Subscribers declare intent once ("watch these 200 companies" or "alert me on manufacturing stress in Haryana") and receive. The system does all the watching.

### Three layers, strict separation
Ingestion (pull data on cadence) → Detection (diff against last state, fire events) → Routing (score, tag CINs, match watchlists, propagate contagion, push alerts). Each layer is independent. A source can be added to ingestion without touching detection or routing.

### Golden Record on CIN
Every data fragment — court case, SARFAESI notice, tender win, director change — must resolve to a CIN before entering the intelligence graph. Unresolved fragments go to queue, not to the main tables.

### Entity resolution: algorithm first, LLM last
Fuzzy matching (pg_trgm, Jaro-Winkler) handles 90%+. LLM only for ambiguous cases at 50-70% confidence. Hard budget: 500 LLM resolutions per month.

### Token efficiency is a hard constraint
18L companies stored locally. Health scores computed locally. External APIs called only when triggered by events. Everything cached. Scraping by recency, not by entity. Diff, don't reprocess.

### Health score recomputes on event, not on schedule
A legal event fires — that CIN's score recomputes. A director resigns — recompute. Don't batch-recompute 18L companies nightly.

### Contagion propagates through director graph
Company flagged RED — all directors' other companies get checked and penalized. Max depth 2. This catches promoter group stress automatically.

### 35 sources, each with three defined properties
Every source has: update frequency (how often it actually changes), ingestion cadence (how often we pull), event definition (what constitutes a change worth firing). Without all three, it's just a scraper.

### Phases
1. Data foundation — OGD load, schema, entity resolution, diff engine, health scores, director graph
2. Event engine — legal sources on cadence, detection, routing, contagion, alerts
3. Full coverage — all 35 sources, workforce signals, tender/trade data
4. Autonomy — self-proposed watchlists, self-healing, pattern learning, the Plays

### Business model is for later
Build the engine first. Detect accurately. Deliver intelligence. How to monetize comes after the engine proves it catches real signals.

### Go to market
Publish one deeply researched free report on a specific company or sector showing what the signal engine catches. Put it on LinkedIn. That single piece of content does more than months of building without audience.

### False positive liability
When the engine starts triggering real-world actions (Play 3 — auto-triggering credit freezes or legal notices), a false positive isn't an unhappy customer — it's a lawsuit. Before Play 3 goes live: E&O insurance, indemnification clauses, confidence thresholds with human-in-the-loop gates, published data latency SLAs.

### CompData is a single point of failure
Backup plan: Tofler API (basic), direct MCA scraping (slow but free), MCA21 document parsing (AOC-4 XBRL), building own enrichment from cached results over time. Goal: self-sufficient on core fields by Phase 4.

### Exit thesis (long-term context)
Acquirers: CRISIL, ICRA, S&P Global India. Comparable: Kensho (acquired by S&P for $550M). The data graph + autonomous detection engine is what makes this an acquisition target, not the UI or the reports.

### GIFT City IFSCA sandbox
Regulatory path for Play 1 (autonomous trade credit underwriting). Allows testing with insurance partner before full IRDAI licensing.

### The three Plays (Phase 4+, after engine is proven)
- **Play 3 first** (no license needed): NBFC connects loan book. System detects stress. Auto-triggers action (sweep account, pause credit line, fire legal notice via partner API). Per-action fee.
- **Play 1 second** (needs IRDAI partner): Trade credit underwriting. Supplier asks "can I extend ₹50L credit to this buyer?" System checks signals, responds in milliseconds.
- **Play 2 third** (needs capital partner): Algorithmic distressed asset acquisition. System detects NCLT admission, auto-generates bid analysis in hours instead of months.

### Data freshness SLA (to be published)
Every source must have documented latency. MCA OGD = monthly (weakest link). NCLT = daily. e-Courts = weekly. If a stress signal fires 45 days after the actual event, it's useless. Latency per source must be tracked and published.

## Competitive landscape

No one in India is doing event-driven autonomous corporate intelligence across the full private company universe.

- **Tofler**: 2M companies, static reports, no events, no alerts, unfunded, 10 employees
- **CMIE Prowess**: ~50K companies, mostly listed, academic pricing, no real-time signals
- **Gridlines/Signzy/Karza**: API wrappers around government portals, point-in-time verification, no continuous monitoring, no intelligence layer
- **OpenCorporates** (global comp): 235M companies, 145 jurisdictions, but no intelligence — just standardized registry data
- **Zephira/Veridion** (global comp): API-first, registry-sourced, AI-enriched, but no India depth

The gap: nobody is watching all 35 Indian government sources continuously, detecting events autonomously, propagating contagion through director networks, and pushing intelligence without being asked.

## Open items

- [ ] Project name
- [ ] OGD data: fresh download or existing
- [ ] Domain name
- [ ] Financial data source: CompData vs MCA scraping vs gradual build
- [ ] Deployment: local now, VPS when needed
