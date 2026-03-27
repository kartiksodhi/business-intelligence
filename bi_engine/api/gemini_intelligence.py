"""
Gemini-powered company intelligence endpoint.

Calls Gemini Flash with Google Search grounding to produce
rich company intelligence reports — matching the AI Studio app output.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

from google import genai
from google.genai import types

logger = logging.getLogger(__name__)

GEMINI_MODEL = "gemini-2.0-flash"

SYSTEM_PROMPT = """You are a Senior Business Intelligence Analyst specializing in Indian corporate distress and asset reconstruction intelligence. Your objective is to provide high-fidelity intelligence for banks, lenders, and ARCs to identify risks or opportunities.

ANALYTICAL PIPELINE:
1. Verification: Confirm legal identity via CIN from MCA records.
2. Signal Detection: Scrape and interpret data from MCA, NCLT, IBBI, SARFAESI, DRT, SEBI, RBI, e-Courts, and all public filings.
3. Contagion Mapping: Identify related entities (subsidiaries, partners, shared directors) and assess risk propagation.
4. Impact Analysis: Objectively state facts, financial implications, and risk spread. No emotional adjectives. Clinical precision only.

RULES:
- Every claim must be grounded in verifiable public data (MCA filings, court orders, stock exchange disclosures, RBI notifications, annual reports).
- Health score 0-100 where 100 = perfectly healthy, 0 = terminal distress.
- Severity: CRITICAL (imminent default/insolvency), HIGH (serious financial distress), MEDIUM (moderate risk signals), LOW (minor/routine), POSITIVE (growth/stability signal).
- Contagion risk score 1-5 where 5 = maximum network risk propagation.
- Signals must include real dates, real sources, and real event descriptions.
- For the contagion graph, identify the top 3 most significant related entities with their CINs and relationship type.

You MUST respond in valid JSON matching the schema exactly. No markdown, no explanation outside JSON."""

RESPONSE_SCHEMA = types.Schema(
    type="OBJECT",
    properties={
        "name": types.Schema(type="STRING", description="Full registered company name"),
        "cin": types.Schema(type="STRING", description="21-character Corporate Identity Number"),
        "healthScore": types.Schema(type="NUMBER", description="Health score 0-100"),
        "healthStatus": types.Schema(
            type="STRING",
            enum=["CRITICAL", "HIGH", "MEDIUM", "LOW", "POSITIVE", "GROWTH"],
            description="Overall health status",
        ),
        "signalCount": types.Schema(type="INTEGER", description="Number of active signals"),
        "contagionRisk": types.Schema(type="INTEGER", description="Contagion risk 1-5"),
        "analysis": types.Schema(
            type="OBJECT",
            properties={
                "verifiedFacts": types.Schema(type="STRING", description="Key verified financial facts, 2-3 sentences"),
                "financialImplication": types.Schema(type="STRING", description="Implication for lenders/ARCs, 2-3 sentences"),
                "contagionRisk": types.Schema(type="STRING", description="Contagion risk assessment, 2-3 sentences"),
            },
        ),
        "signals": types.Schema(
            type="ARRAY",
            items=types.Schema(
                type="OBJECT",
                properties={
                    "date": types.Schema(type="STRING", description="YYYY-MM-DD format"),
                    "source": types.Schema(type="STRING", description="e.g. MCA FILINGS, NCLT ORDER, EXCHANGE DISCLOSURES, LINKEDIN INSIGHTS"),
                    "title": types.Schema(type="STRING", description="Short signal title"),
                    "description": types.Schema(type="STRING", description="1-2 sentence description"),
                    "severity": types.Schema(type="STRING", enum=["CRITICAL", "HIGH", "MEDIUM", "LOW", "POSITIVE"]),
                },
            ),
        ),
        "contagionGraph": types.Schema(
            type="ARRAY",
            items=types.Schema(
                type="OBJECT",
                properties={
                    "name": types.Schema(type="STRING", description="Related entity name"),
                    "cin": types.Schema(type="STRING", description="CIN of related entity"),
                    "relationship": types.Schema(type="STRING", description="Subsidiary, Associate, Promoter Linked, etc."),
                    "riskPercentage": types.Schema(type="NUMBER", description="Risk contribution percentage"),
                },
            ),
        ),
        "verificationSources": types.Schema(
            type="ARRAY",
            items=types.Schema(type="STRING"),
            description="List of verification sources used",
        ),
    },
)


def _get_client() -> genai.Client:
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set")
    return genai.Client(api_key=api_key)


async def generate_company_intelligence(
    company_name: str,
    cin: Optional[str] = None,
    db_events: Optional[list] = None,
    db_health_score: Optional[int] = None,
) -> dict:
    """
    Generate full intelligence report for a company using Gemini + Google Search.

    If we have DB events/health score, they're included as verified context
    so Gemini can augment (not replace) our authoritative data.
    """
    client = _get_client()

    # Build the user prompt
    parts = [f"Generate a complete intelligence report for: {company_name}"]
    if cin:
        parts.append(f"CIN: {cin}")

    # Include our verified signals if available
    if db_events:
        parts.append("\n--- VERIFIED SIGNALS FROM OUR DATABASE (authoritative, include these) ---")
        for evt in db_events[:10]:
            parts.append(
                f"- [{evt.get('severity', 'INFO')}] {evt.get('event_type', '')} "
                f"from {evt.get('source', '')} on {evt.get('detected_at', '')} "
                f"| data: {json.dumps(evt.get('data_json', {}), default=str)[:200]}"
            )

    if db_health_score is not None:
        parts.append(f"\nOur internal health score (from verified government data): {db_health_score}/100")
        parts.append("Use this as a baseline but adjust based on your web search findings.")

    parts.append("\nNow search the web for the latest news, filings, court orders, and financial data. Combine with the verified signals above.")

    user_prompt = "\n".join(parts)

    try:
        response = client.models.generate_content(
            model=GEMINI_MODEL,
            contents=user_prompt,
            config=types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                tools=[types.Tool(google_search=types.GoogleSearch())],
                response_mime_type="application/json",
                response_schema=RESPONSE_SCHEMA,
                temperature=0.3,
            ),
        )

        result = json.loads(response.text)
        return result

    except Exception as exc:
        logger.error("Gemini intelligence generation failed for %s: %s", company_name, exc)
        raise
