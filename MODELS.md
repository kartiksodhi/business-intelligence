# MODELS.md

## Model routing table

| Task type | Model | Why |
|---|---|---|
| classify / tag / extract_fields | `claude-haiku-4-5` | Cheapest, fast, structured output |
| bulk_parse / summarize / HTML scrape | `gemini-2.5-flash` | 1M context, cheap per token |
| score / pattern detection / report generation | `claude-sonnet-4-6` | Best reasoning per cost |
| long_doc / deep_research / 100+ page filings | `gemini-2.5-pro` | 1M context window |
| code_generation | Codex (GPT-4o) | Per agent role spec in CLAUDE.md |
| graph_traversal / regex / date parsing | Pure Python — no LLM | Zero cost, deterministic |

**Token savings vs routing everything to Sonnet: 60–70%.**

## Hard routing rules
1. Never send graph traversal or regex work to an LLM. Python only.
2. Chunk HTML before any LLM call. Never send raw full-page HTML.
3. Always request JSON output. Never free-text where structured data is expected.
4. Cache repeated system prompts. Do not re-send identical context on every call.
5. Classify first with Haiku. Only escalate to Sonnet/Pro if Haiku confidence < threshold.

## Cost per 1M tokens (update quarterly)
| Model | Input | Output |
|---|---|---|
| claude-haiku-4-5 | $0.80 | $4.00 |
| claude-sonnet-4-6 | $3.00 | $15.00 |
| gemini-2.5-flash | $0.15 | $0.60 |
| gemini-2.5-pro | $1.25 | $10.00 |

*Last updated: March 2026*

## API key management
- Anthropic key: `ANTHROPIC_API_KEY` in `bi_engine/.env` (never commit)
- Gemini key: `GEMINI_API_KEY` in `bi_engine/.env` (never commit)
- Max Claude API calls: 500/month (entity resolution LLM fallback + alert summaries only)
- Gemini: no hard cap, but monitor daily via operator CLI `/cost`

## Caching strategy
- System prompts: cache with `cache_control: ephemeral` on Anthropic API (saves ~90% on repeated calls)
- Entity resolution: cache resolved CIN lookups in PostgreSQL `entity_aliases` table — never re-call LLM for known entity
- Scraper HTML: store raw snapshot per source per run — diff against previous before any LLM processing

## Gemini SDK (quick ref)
```python
pip install google-genai

from google import genai
client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))

# Flash — bulk parsing, HTML, cheap
client.models.generate_content(model="gemini-2.5-flash", contents=prompt)

# Pro — long documents, deep research
client.models.generate_content(model="gemini-2.5-pro", contents=prompt)
```

## Alert summary routing
Alert summaries → Sonnet only. Called once per batched alert group at delivery time. Never pre-generated. See CLAUDE.md constraint #6.
