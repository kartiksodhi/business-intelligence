# CLAUDE.md

## What this is
A signal network. Not a database. Watches 35 Indian government sources. Detects events. Connects signals. Fires intelligence before any competitor database reflects the change. 18 lakh+ companies stored locally.

## Architecture — three layers only
```
INGESTION → DETECTION → ROUTING
```
- **Ingestion**: Pull each source on its own cadence. Land in PostgreSQL. No human triggers.
- **Detection**: Diff against last known state. Only fire when something changed.
- **Routing**: Score event. Tag CIN. Check contagion. Match watchlists. Push alert.

Read PIPELINE.md for full step-by-step detail on each layer.

## Golden Record
Every data fragment resolves to a CIN before entering the graph. Unresolved = queue, not main tables. CIN is the only primary key that matters.

## Agent roles
| Agent | Owns | Never touches |
|---|---|---|
| Claude Code | Architecture, entity resolution, detection, routing, feedback loop, operator CLI, all specs | Frontend, routine scrapers |
| Codex | All scraper modules, DB schema, FastAPI endpoints, health scoring — always to Claude Code spec | Architecture decisions, entity resolution logic |
| Gemini | All React/Next.js UI — dashboard, watchlist, company profile, source monitor | Backend |
| Playwright | Government portal automation — free, install at project start | Commercial JS sites |
| Claude API | Entity resolution LLM fallback (max 500/month) + alert summaries only | Everything else |

## Tools — what is installed and what is not
- **Playwright** — INSTALL at project start. Free. Handles all government portals.
- **BeautifulSoup** — INSTALL at project start. Free. Simple static HTML.
- **pytesseract** — INSTALL at project start. Free. CAPTCHA OCR.
- **Firecrawl** — NOT INSTALLED. Paid per crawl. Only install when a specific source genuinely cannot be handled by Playwright. Requires operator approval before installing. Use for Naukri, Glassdoor, career pages only if needed.

## Hard constraints — never break these
1. OGD CSV = free local base. Never hit external API for base company data.
2. Playwright before Firecrawl. Free before paid. Always.
3. Algorithm before LLM. Fuzzy match handles 90%+. LLM max 500/month.
4. Diff not reprocess. Store state per source. Process only delta.
5. Health score recomputes on event only. Never on schedule.
6. AI summary only at alert delivery. Never pre-generated.
7. Scrape by recency not entity. New cases this week, not 18L searches.
8. Deduplicate before AI summary. Batch multi-event alerts first.
9. Any day over ₹500 external API cost = operator review triggered.

## Phases
- **Phase 1**: OGD load, schema, entity resolution, diff engine, health scores, director graph, operator CLI, daily digest, Telegram bot.
- **Phase 2**: e-Courts, NCLT, DRT, SARFAESI, IBBI on cadence. Detection + routing + contagion + deduplication. Alert dashboard (Gemini).
- **Phase 3**: All 35 sources. GST/EPFO mapping via AOC-4. Shell detection. Cluster detection. CompData triggered enrichment. Subscriber system. Monthly recalibration.
- **Phase 4**: Automated recalibration. Self-healing scrapers. A2A plays.
- **CDM trigger**: When MCA CDM structured API goes live — pause all else, integrate immediately. Drops MCA latency from 30 days to near real-time.

## Operator communication — four channels
1. **Dashboard** — Gemini builds. WebSocket real-time. See PIPELINE.md for screens.
2. **Daily digest** — 7am email via Resend. Top 5 events, costs, accuracy, scraper status.
3. **Operator CLI** — FastAPI `/op/` endpoints. See PIPELINE.md for endpoint list.
4. **Telegram bot** — CRITICAL severity only. Immediate push. Free.

## Spec files — read only when directly needed
`PIPELINE.md` · `SKILLS.md` · `SOURCES.md` · `MEMORY.md` · `INTELLIGENCE.md` · `COMPLIANCE.md` · `REGULATIONS.md` · `SECURITY.md` · `MODELS.md`

**Do NOT read any spec file proactively.** Only read a file if the current task explicitly requires data from it. If CLAUDE.md context is sufficient, stop there.

## Token efficiency — hard rules for every session
1. **One task per response.** Complete the requested task, then stop. Do not start the next task.
2. **No background subagents** unless the user explicitly says "run in background" or "run in parallel."
3. **No speculative file reads.** Do not read a file "just to check." Read only what the current task requires.
4. **No broad exploration.** If a prompt says "fix everything" or "investigate", ask which specific thing to fix first.
5. **No repeated reads.** If a file was read earlier in the session, use that result. Do not re-read it.
6. **Answer from context first.** If CLAUDE.md or memory already answers the question, do not open any file.

## Mistakes → rules (Boris Cherny pattern)
Every time Claude does something wrong in ICIE, add it as a rule to CLAUDE.md immediately. Do not wait. Do not batch. This compounds — the longer we build, the smarter the agent gets.

## Parallel sessions pattern
Run 10–15 concurrent Claude Code sessions across pipeline phases simultaneously. One session per major domain: MCA scraper, eCourts scraper, entity resolution, scoring, routing, RERA, dashboard. Each owns its domain fully. Separate terminal tabs, not worktrees — fully independent cognition per phase.

## When uncertain
Claude Code states uncertainty and asks. Never guesses on architecture. Never proceeds without a spec.
