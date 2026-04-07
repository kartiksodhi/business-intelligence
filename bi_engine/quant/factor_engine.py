"""
Alpha factor engine.

Computes 10 alpha factors for each CIN in ticker_bridge and writes daily
snapshots to factor_scores. All data comes from existing ICIE tables — no
external API calls.

Called daily at 5:00 PM IST by quant_scheduler.py, and on-demand via
POST /quant/factors/compute.

Factors:
  1. health_score_raw            — current health score
  2. health_score_30d_delta      — 30-day change in health score
  3. health_score_90d_delta      — 90-day change in health score
  4. legal_velocity_score        — Poisson anomaly P(k) for legal case rate
  5. director_instability_score  — director change rate z-score
  6. filing_decay_score          — inverted filing freshness (high = bad)
  7. workforce_momentum          — EPFO headcount delta 90d
  8. government_revenue_signal   — GeM order wins 180d (normalised)
  9. leverage_creep_signal       — new charge registrations 90d
 10. distress_composite          — weighted sum of active distress signals
 11. promoter_stress_signal      — SEBI bulk deal sell direction
 12. sector_stress_percentile    — health score percentile within NIC+state peer group

composite_alpha_score = weighted sum, normalized to z-score across listed universe.
"""

from __future__ import annotations

import logging
import math
from datetime import date, timedelta
from typing import Optional

import asyncpg
import numpy as np
import pandas as pd

from quant.cin_bridge import get_all_listed_cins

logger = logging.getLogger(__name__)

# Distress signal weights for composite
_DISTRESS_WEIGHTS: dict[str, float] = {
    "SARFAESI_AUCTION":   5.0,
    "SARFAESI_POSSESSION": 4.0,
    "SARFAESI_DEMAND":    3.5,
    "NCLT_ADMISSION":     4.0,
    "NCLT_LIQUIDATION":   5.0,
    "DRT_APPLICATION":    2.5,
    "ECOURTS_SEC138":     2.0,
    "GST_CANCELLED":      2.0,
    "GST_SUSPENDED":      1.5,
    "FILING_DECAY":       1.0,
    "RBI_WILFUL_DEFAULTER": 5.0,
    "EPFO_CONTRIBUTION_DROP": 2.0,
    "EPFO_ESTABLISHMENT_DELISTED": 3.0,
}

# Composite factor weights (sum should ≈ 1.0)
_FACTOR_WEIGHTS: dict[str, float] = {
    "distress_composite":         -0.25,
    "health_score_momentum_30d":  +0.15,
    "health_score_momentum_90d":  +0.10,
    "workforce_momentum":         +0.15,
    "promoter_stress_signal":     -0.15,
    "legal_velocity_score":       -0.10,
    "government_revenue_signal":  +0.05,
    "leverage_creep_signal":      -0.05,
    "director_instability_score": -0.03,
    "filing_decay_score":         -0.02,
    "sector_stress_percentile":   -0.01,
}


async def compute_all_factors(pool: asyncpg.Pool, score_date: Optional[date] = None) -> int:
    """
    Compute factor scores for all CINs in ticker_bridge.
    Returns number of CINs processed.
    """
    if score_date is None:
        score_date = date.today()

    async with pool.acquire() as conn:
        cins = await get_all_listed_cins(conn)

    if not cins:
        logger.warning("No CINs in ticker_bridge — run build_cin_bridge.py first")
        return 0

    logger.info("Computing factors for %d listed CINs on %s", len(cins), score_date)

    raw_rows: list[dict] = []
    async with pool.acquire() as conn:
        for cin in cins:
            row = await _compute_cin_factors(conn, cin, score_date)
            raw_rows.append(row)

    # Cross-sectional normalization
    normalized = _normalize_composite(raw_rows)

    # Persist
    async with pool.acquire() as conn:
        await _upsert_factor_scores(conn, normalized, score_date)

    logger.info("Factor computation complete for %d CINs", len(normalized))
    return len(normalized)


async def _compute_cin_factors(
    conn: asyncpg.Connection,
    cin: str,
    score_date: date,
) -> dict:
    health_raw, hs_30d, hs_90d = await _health_score_data(conn, cin, score_date)
    legal_vel   = await _legal_velocity_score(conn, cin)
    dir_instab  = await _director_instability(conn, cin)
    filing_dec  = await _filing_decay(conn, cin)
    workforce   = await _workforce_momentum(conn, cin)
    gem_rev     = await _government_revenue(conn, cin)
    leverage    = await _leverage_creep(conn, cin)
    distress    = await _distress_composite(conn, cin)
    promoter    = await _promoter_stress(conn, cin)
    sector_pct  = await _sector_stress_percentile(conn, cin)
    signals     = await _active_combination_signals(conn, cin)

    return {
        "cin": cin,
        "health_score_raw":            health_raw,
        "health_score_30d_delta":      hs_30d,
        "health_score_90d_delta":      hs_90d,
        "legal_velocity_score":        legal_vel,
        "director_instability_score":  dir_instab,
        "filing_decay_score":          filing_dec,
        "workforce_momentum":          workforce,
        "government_revenue_signal":   gem_rev,
        "leverage_creep_signal":       leverage,
        "distress_composite":          distress,
        "promoter_stress_signal":      promoter,
        "sector_stress_percentile":    sector_pct,
        "signal_fired":                signals,
    }


# ─── Individual factor computations ───────────────────────────────────────────

async def _health_score_data(
    conn: asyncpg.Connection,
    cin: str,
    score_date: date,
) -> tuple[Optional[int], Optional[float], Optional[float]]:
    """Returns (current_score, 30d_delta, 90d_delta)."""
    row = await conn.fetchrow(
        "SELECT health_score FROM master_entities WHERE cin = $1", cin
    )
    if not row or row["health_score"] is None:
        return None, None, None

    current = int(row["health_score"])

    async def _score_n_days_ago(days: int) -> Optional[int]:
        hist = await conn.fetchrow(
            """
            SELECT (data_json->>'health_score_after')::int AS score
            FROM events
            WHERE cin = $1
              AND event_type = 'HEALTH_SCORE_CHANGED'
              AND data_json ? 'health_score_after'
              AND detected_at::date <= $2 - ($3 || ' days')::INTERVAL
            ORDER BY detected_at DESC
            LIMIT 1
            """,
            cin, score_date, str(days),
        )
        return int(hist["score"]) if hist and hist["score"] is not None else None

    score_30 = await _score_n_days_ago(30)
    score_90 = await _score_n_days_ago(90)

    delta_30 = float(current - score_30) if score_30 is not None else None
    delta_90 = float(current - score_90) if score_90 is not None else None

    return current, delta_30, delta_90


async def _legal_velocity_score(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """
    Poisson anomaly: P(k) where k = new legal cases in last 30d,
    λ = average monthly cases over prior 12 months.
    Returns raw P(k). Low value = anomalous spike (bad).
    Returns None if insufficient history.
    """
    k_row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM legal_events
        WHERE cin = $1
          AND filing_date >= CURRENT_DATE - INTERVAL '30 days'
        """,
        cin,
    )
    k = int(k_row["cnt"]) if k_row else 0

    baseline_row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM legal_events
        WHERE cin = $1
          AND filing_date >= CURRENT_DATE - INTERVAL '365 days'
          AND filing_date <  CURRENT_DATE - INTERVAL '30 days'
        """,
        cin,
    )
    total_11m = int(baseline_row["cnt"]) if baseline_row else 0

    if total_11m == 0:
        return None  # No history — can't compute

    lam = total_11m / 11.0  # avg cases per month

    if lam == 0:
        return 1.0 if k == 0 else 0.01

    # Poisson PMF: P(X=k) = (λ^k * e^-λ) / k!
    try:
        log_p = k * math.log(lam) - lam - sum(math.log(i) for i in range(1, k + 1))
        p = math.exp(log_p)
    except (ValueError, OverflowError):
        p = 0.001

    return round(p, 6)


async def _director_instability(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """Count director appointments + cessations in last 90 days (raw count)."""
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM governance_graph
        WHERE cin = $1
          AND (
            date_of_appointment >= CURRENT_DATE - INTERVAL '90 days'
            OR cessation_date    >= CURRENT_DATE - INTERVAL '90 days'
          )
        """,
        cin,
    )
    return float(row["cnt"]) if row else 0.0


async def _filing_decay(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """
    Inverted filing freshness: 0 = fresh (good), 100 = very stale (bad).
    Mirrors health_scorer._compute_filing_freshness() but inverted.
    """
    row = await conn.fetchrow(
        "SELECT date_of_last_agm, status FROM master_entities WHERE cin = $1", cin
    )
    if not row:
        return 50.0

    agm_date = row["date_of_last_agm"]
    status   = row["status"]

    if agm_date is None:
        return 70.0 if status == "Active" else 100.0

    months_ago = (date.today() - agm_date).days / 30.44
    if months_ago < 12:
        return 0.0
    if months_ago < 18:
        return 30.0
    if months_ago < 24:
        return 60.0
    if months_ago < 36:
        return 85.0
    return 100.0


async def _workforce_momentum(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """
    Net EPFO signal over last 90 days.
    +1 per HIRING_SURGE event, -1 per CONTRIBUTION_DROP event.
    Normalized to [-5, +5].
    """
    rows = await conn.fetch(
        """
        SELECT event_type
        FROM events
        WHERE cin = $1
          AND event_type IN ('EPFO_HIRING_SURGE', 'EPFO_CONTRIBUTION_DROP',
                             'EPFO_ESTABLISHMENT_DELISTED')
          AND detected_at >= NOW() - INTERVAL '90 days'
        """,
        cin,
    )
    score = 0.0
    for r in rows:
        if r["event_type"] == "EPFO_HIRING_SURGE":
            score += 1.0
        elif r["event_type"] == "EPFO_CONTRIBUTION_DROP":
            score -= 1.0
        elif r["event_type"] == "EPFO_ESTABLISHMENT_DELISTED":
            score -= 2.0
    return max(-5.0, min(5.0, score))


async def _government_revenue(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """
    Count of GeM order wins in last 180 days.
    Simple count — proxy for government revenue pipeline.
    """
    row = await conn.fetchrow(
        """
        SELECT COUNT(*) AS cnt
        FROM events
        WHERE cin = $1
          AND event_type = 'GEM_ORDER_WON'
          AND detected_at >= NOW() - INTERVAL '180 days'
        """,
        cin,
    )
    return float(row["cnt"]) if row else 0.0


async def _leverage_creep(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """
    Net charge activity in last 90 days.
    +1 per CHARGE_CREATED, -0.5 per CHARGE_SATISFIED (debt being paid off).
    """
    rows = await conn.fetch(
        """
        SELECT event_type
        FROM events
        WHERE cin = $1
          AND event_type IN ('CHARGE_CREATED', 'MCA_CHARGE_SATISFIED')
          AND detected_at >= NOW() - INTERVAL '90 days'
        """,
        cin,
    )
    score = 0.0
    for r in rows:
        if r["event_type"] == "CHARGE_CREATED":
            score += 1.0
        else:
            score -= 0.5
    return max(0.0, score)


async def _distress_composite(conn: asyncpg.Connection, cin: str) -> float:
    """
    Weighted sum of active distress signals from events + legal_events.
    Higher = more distressed (negative for portfolio construction).
    """
    # Recent events (last 180 days)
    event_rows = await conn.fetch(
        """
        SELECT event_type
        FROM events
        WHERE cin = $1
          AND event_type = ANY($2::text[])
          AND detected_at >= NOW() - INTERVAL '180 days'
        """,
        cin,
        list(_DISTRESS_WEIGHTS.keys()),
    )

    # Active legal cases
    legal_rows = await conn.fetch(
        """
        SELECT case_type
        FROM legal_events
        WHERE cin = $1
          AND status NOT IN ('Disposed', 'Dismissed', 'Withdrawn', 'Closed')
        """,
        cin,
    )

    seen: set[str] = set()
    score = 0.0

    for r in event_rows:
        et = r["event_type"]
        if et not in seen:
            score += _DISTRESS_WEIGHTS.get(et, 0.0)
            seen.add(et)

    _LEGAL_MAP = {
        "SARFAESI_AUCTION": "SARFAESI_AUCTION",
        "SARFAESI_13_4":    "SARFAESI_POSSESSION",
        "SARFAESI_13_2":    "SARFAESI_DEMAND",
        "NCLT_7":           "NCLT_ADMISSION",
        "NCLT_9":           "NCLT_ADMISSION",
        "NCLT_10":          "NCLT_LIQUIDATION",
        "DRT":              "DRT_APPLICATION",
        "SEC_138":          "ECOURTS_SEC138",
    }
    for r in legal_rows:
        mapped = _LEGAL_MAP.get(r["case_type"], r["case_type"])
        if mapped not in seen:
            score += _DISTRESS_WEIGHTS.get(mapped, 0.5)
            seen.add(mapped)

    return round(score, 2)


async def _promoter_stress(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """
    SEBI bulk deal signal in last 90 days.
    Positive = promoter buying (bullish), Negative = promoter selling (bearish).
    """
    rows = await conn.fetch(
        """
        SELECT event_type
        FROM events
        WHERE cin = $1
          AND event_type IN ('SEBI_BULK_DEAL_PROMOTER_SELL', 'SEBI_BULK_DEAL_PROMOTER_BUY',
                             'SEBI_BULK_DEAL', 'SEBI_BLOCK_DEAL')
          AND detected_at >= NOW() - INTERVAL '90 days'
        """,
        cin,
    )
    score = 0.0
    for r in rows:
        if r["event_type"] == "SEBI_BULK_DEAL_PROMOTER_BUY":
            score += 1.0
        elif r["event_type"] == "SEBI_BULK_DEAL_PROMOTER_SELL":
            score -= 1.5
        elif r["event_type"] == "SEBI_BULK_DEAL":
            score -= 0.5  # unknown direction = mild negative
    return max(-5.0, min(5.0, score))


async def _sector_stress_percentile(conn: asyncpg.Connection, cin: str) -> Optional[float]:
    """
    Health score percentile within NIC+state peer group.
    0 = most stressed (bottom), 100 = healthiest (top).
    """
    row = await conn.fetchrow(
        "SELECT health_score, industrial_class, state FROM master_entities WHERE cin = $1", cin
    )
    if not row or row["health_score"] is None:
        return 50.0

    my_score = int(row["health_score"])
    nic = row["industrial_class"]
    state = row["state"]

    if not nic or not state:
        return 50.0

    peer_rows = await conn.fetch(
        """
        SELECT health_score
        FROM master_entities
        WHERE industrial_class = $1
          AND state = $2
          AND health_score IS NOT NULL
        """,
        nic, state,
    )
    if not peer_rows:
        return 50.0

    scores = [int(r["health_score"]) for r in peer_rows]
    below  = sum(1 for s in scores if s < my_score)
    percentile = (below / len(scores)) * 100.0
    return round(percentile, 1)


async def _active_combination_signals(conn: asyncpg.Connection, cin: str) -> list[str]:
    """Return names of any active signal combinations fired in last 30 days."""
    from detection.signal_combiner import COMBINATIONS
    combination_names = [c["name"] for c in COMBINATIONS]

    rows = await conn.fetch(
        """
        SELECT DISTINCT event_type
        FROM events
        WHERE cin = $1
          AND event_type = ANY($2::text[])
          AND detected_at >= NOW() - INTERVAL '30 days'
        """,
        cin,
        combination_names,
    )
    return [r["event_type"] for r in rows]


# ─── Cross-sectional normalization ─────────────────────────────────────────────

def _normalize_composite(rows: list[dict]) -> list[dict]:
    """
    Compute composite_alpha_score as weighted z-score across the listed universe.
    Each factor is first z-scored (mean=0, std=1), then weighted and summed.
    Final composite is re-z-scored to [-3, +3].
    """
    if not rows:
        return rows

    df = pd.DataFrame(rows)

    factor_cols = [
        "distress_composite",
        "health_score_30d_delta",
        "health_score_90d_delta",
        "workforce_momentum",
        "promoter_stress_signal",
        "legal_velocity_score",
        "government_revenue_signal",
        "leverage_creep_signal",
        "director_instability_score",
        "filing_decay_score",
        "sector_stress_percentile",
    ]

    for col in factor_cols:
        if col not in df.columns:
            df[col] = 0.0
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        std = df[col].std()
        if std > 0:
            df[col + "_z"] = (df[col] - df[col].mean()) / std
        else:
            df[col + "_z"] = 0.0

    # Composite = weighted sum of z-scores
    composite = pd.Series(0.0, index=df.index)

    weight_map = {
        "distress_composite_z":         _FACTOR_WEIGHTS["distress_composite"],
        "health_score_30d_delta_z":     _FACTOR_WEIGHTS["health_score_momentum_30d"],
        "health_score_90d_delta_z":     _FACTOR_WEIGHTS["health_score_momentum_90d"],
        "workforce_momentum_z":         _FACTOR_WEIGHTS["workforce_momentum"],
        "promoter_stress_signal_z":     _FACTOR_WEIGHTS["promoter_stress_signal"],
        "legal_velocity_score_z":       _FACTOR_WEIGHTS["legal_velocity_score"],
        "government_revenue_signal_z":  _FACTOR_WEIGHTS["government_revenue_signal"],
        "leverage_creep_signal_z":      _FACTOR_WEIGHTS["leverage_creep_signal"],
        "director_instability_score_z": _FACTOR_WEIGHTS["director_instability_score"],
        "filing_decay_score_z":         _FACTOR_WEIGHTS["filing_decay_score"],
        "sector_stress_percentile_z":   _FACTOR_WEIGHTS["sector_stress_percentile"],
    }

    for z_col, weight in weight_map.items():
        if z_col in df.columns:
            composite += df[z_col] * weight

    # Re-normalize composite to z-score, clip to [-3, +3]
    std_c = composite.std()
    if std_c > 0:
        composite = (composite - composite.mean()) / std_c
    composite = composite.clip(-3.0, 3.0).round(4)

    df["composite_alpha_score"] = composite

    # Convert back to list of dicts (drop _z columns)
    z_cols = [c for c in df.columns if c.endswith("_z")]
    df = df.drop(columns=z_cols)

    return df.to_dict(orient="records")


async def _upsert_factor_scores(
    conn: asyncpg.Connection,
    rows: list[dict],
    score_date: date,
) -> None:
    await conn.executemany(
        """
        INSERT INTO factor_scores
            (cin, score_date, health_score_raw, health_score_30d_delta, health_score_90d_delta,
             legal_velocity_score, director_instability_score, filing_decay_score,
             workforce_momentum, government_revenue_signal, leverage_creep_signal,
             distress_composite, promoter_stress_signal, sector_stress_percentile,
             composite_alpha_score, signal_fired, computed_at)
        VALUES
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,NOW())
        ON CONFLICT (cin, score_date) DO UPDATE
            SET health_score_raw           = EXCLUDED.health_score_raw,
                health_score_30d_delta     = EXCLUDED.health_score_30d_delta,
                health_score_90d_delta     = EXCLUDED.health_score_90d_delta,
                legal_velocity_score       = EXCLUDED.legal_velocity_score,
                director_instability_score = EXCLUDED.director_instability_score,
                filing_decay_score         = EXCLUDED.filing_decay_score,
                workforce_momentum         = EXCLUDED.workforce_momentum,
                government_revenue_signal  = EXCLUDED.government_revenue_signal,
                leverage_creep_signal      = EXCLUDED.leverage_creep_signal,
                distress_composite         = EXCLUDED.distress_composite,
                promoter_stress_signal     = EXCLUDED.promoter_stress_signal,
                sector_stress_percentile   = EXCLUDED.sector_stress_percentile,
                composite_alpha_score      = EXCLUDED.composite_alpha_score,
                signal_fired               = EXCLUDED.signal_fired,
                computed_at                = NOW()
        """,
        [
            (
                r["cin"], score_date,
                r.get("health_score_raw"),
                r.get("health_score_30d_delta"),
                r.get("health_score_90d_delta"),
                r.get("legal_velocity_score"),
                r.get("director_instability_score"),
                r.get("filing_decay_score"),
                r.get("workforce_momentum"),
                r.get("government_revenue_signal"),
                r.get("leverage_creep_signal"),
                r.get("distress_composite"),
                r.get("promoter_stress_signal"),
                r.get("sector_stress_percentile"),
                r.get("composite_alpha_score"),
                r.get("signal_fired") or [],
            )
            for r in rows
        ],
    )
