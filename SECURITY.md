# SECURITY.md

## Data classification

**Public (no restrictions)**: OGD CSV, court case listings, NCLT/DRT/IBBI orders, SARFAESI auction notices, SEBI deal data, RBI publications, GeM/CPPP tender data, Udyam registrations, MOEF clearances, CCI filings. All published by government for public use.

**Semi-public (use with care)**: CompData enrichment (subject to their ToS). Firecrawl results from job portals (public pages, respect robots.txt). Glassdoor reviews (public, ToS varies).

**Sensitive (protect)**: Subscriber watchlists. Alert history. Entity resolution training data.

**Never collect**: Aadhaar. Individual PAN. Bank account details. Biometric data. Data from behind login walls without authorization. Personal social media profiles.

## Scraping rules

1. Only scrape publicly available information
2. Respect robots.txt on every domain
3. Rate limit: max 1 request per 2 seconds per source
4. If a site blocks us, stop. Find alternatives. Don't bypass.
5. Never create fake accounts for access
6. Cache aggressively to minimize crawl frequency
7. SARFAESI/auction portals: public notices only
8. e-Courts: case listings only, not sealed/restricted
9. Firecrawl sources: company pages only, never individual profiles

## Database security

- PostgreSQL: password-protected, not network-exposed in local dev
- Production: SSL only, no public port
- Separate DB users: `reader` (API serving), `writer` (ingestion), `admin` (schema)
- Daily encrypted backups. OGD snapshots retained permanently.

## Secrets management

All API keys in environment variables, never in code.
```
DATABASE_URL, COMPDATA_API_KEY, CLAUDE_API_KEY, FIRECRAWL_API_KEY
```
`.env` in `.gitignore`. Always.

## Incident response

**Source blocks us**: Stop immediately. Find alternative. Don't circumvent.
**API key compromised**: Rotate immediately. Audit usage. Fix leak.
**Bad data detected**: Quarantine affected records. Trace to source. Fix pipeline. Re-process.
