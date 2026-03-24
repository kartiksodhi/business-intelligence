"""
Health scoring engine.

Recomputes the composite health score for a company (CIN) from local DB data only.
Called on every event that changes underlying company state. Never called on a schedule.

Architecture: HealthScorer -> ContagionPropagator (calls HealthScorer recursively, max depth 2)
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, List, Optional

import asyncpg


logger = logging.getLogger(__name__)

WEIGHTS = {
    "filing_freshness": 0.25,
    "director_stability": 0.20,
    "legal_risk": 0.25,
    "financial_health": 0.20,
    "capital_trajectory": 0.10,
}

ACTIVE_LEGAL_STATUSES_EXCLUDE = frozenset(
    ["Disposed", "Dismissed", "Withdrawn", "Closed"]
)


@dataclass
class ScoreResult:
    cin: str
    score: float
    band: str
    previous_score: float
    previous_band: str
    components: Dict[str, Dict[str, float]]
    contagion_penalty: float
    contagion_sources: List[str]
    triggering_event_id: int
    computed_at: datetime


class HealthScorer:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def recompute(self, cin: str, triggering_event_id: int) -> ScoreResult:
        async with self.db.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT health_score, health_band FROM master_entities WHERE cin = $1",
                cin,
            )
            previous_score = (
                float(row["health_score"])
                if row and row["health_score"] is not None
                else 50.0
            )
            previous_band = row["health_band"] if row and row["health_band"] else "AMBER"

        (
            filing_freshness,
            director_stability,
            legal_risk,
            financial_health,
            capital_trajectory,
        ) = await asyncio.gather(
            self._compute_filing_freshness(cin),
            self._compute_director_stability(cin),
            self._compute_legal_risk(cin),
            self._compute_financial_health(cin),
            self._compute_capital_trajectory(cin),
        )

        raw_scores = {
            "filing_freshness": filing_freshness,
            "director_stability": director_stability,
            "legal_risk": legal_risk,
            "financial_health": financial_health,
            "capital_trajectory": capital_trajectory,
        }
        raw_score = sum(raw_scores[key] * WEIGHTS[key] for key in WEIGHTS)

        final_score, contagion_penalty, contagion_sources = await self._apply_contagion_penalty(
            cin, raw_score
        )
        final_score = round(final_score, 1)
        band = await self._get_band(final_score)

        components = {
            key: {
                "raw": raw_scores[key],
                "weight": WEIGHTS[key],
                "weighted": round(raw_scores[key] * WEIGHTS[key], 4),
            }
            for key in WEIGHTS
        }

        await self._persist_score(
            cin,
            final_score,
            band,
            components,
            triggering_event_id,
            previous_score,
            previous_band,
        )

        return ScoreResult(
            cin=cin,
            score=final_score,
            band=band,
            previous_score=previous_score,
            previous_band=previous_band,
            components=components,
            contagion_penalty=contagion_penalty,
            contagion_sources=contagion_sources,
            triggering_event_id=triggering_event_id,
            computed_at=datetime.utcnow(),
        )

    async def _compute_filing_freshness(self, cin: str) -> float:
        try:
            row = await self.db.fetchrow(
                "SELECT date_of_last_agm, status FROM master_entities WHERE cin = $1",
                cin,
            )
            if row is None:
                return 30.0

            agm_date: Optional[date] = row["date_of_last_agm"]
            status = row["status"]

            if agm_date is None:
                return 30.0 if status == "Active" else 0.0

            months_ago = (date.today() - agm_date).days / 30.44
            if months_ago < 12:
                return 100.0
            if months_ago < 18:
                return 70.0
            if months_ago < 24:
                return 40.0
            if months_ago < 36:
                return 15.0
            return 0.0
        except Exception as exc:
            logger.error("HealthScorer filing_freshness failed for %s: %s", cin, exc)
            return 50.0

    async def _compute_director_stability(self, cin: str) -> float:
        try:
            row = await self.db.fetchrow(
                """
                SELECT COUNT(*) AS change_count
                FROM governance_graph
                WHERE cin = $1
                  AND (
                    date_of_appointment >= NOW() - INTERVAL '90 days'
                    OR cessation_date >= NOW() - INTERVAL '90 days'
                  )
                """,
                cin,
            )
            count = int(row["change_count"]) if row else 0

            if count == 0:
                return 100.0
            if count == 1:
                return 80.0
            if count == 2:
                return 50.0
            return 20.0
        except Exception as exc:
            logger.error("HealthScorer director_stability failed for %s: %s", cin, exc)
            return 50.0

    async def _compute_legal_risk(self, cin: str) -> float:
        try:
            rows = await self.db.fetch(
                """
                SELECT case_type, COUNT(*) AS cnt
                FROM legal_events
                WHERE cin = $1
                  AND status NOT IN ('Disposed', 'Dismissed', 'Withdrawn', 'Closed')
                GROUP BY case_type
                """,
                cin,
            )

            if not rows:
                return 100.0

            case_counts: Dict[str, int] = {
                row["case_type"]: int(row["cnt"]) for row in rows
            }
            total_active = sum(case_counts.values())

            priority_types = {
                "SARFAESI_AUCTION",
                "SARFAESI_13_4",
                "NCLT_7",
                "NCLT_9",
                "NCLT_10",
            }
            if any(case_type in case_counts for case_type in priority_types):
                return 5.0
            if total_active >= 3:
                return 20.0

            sec138_count = case_counts.get("SEC_138", 0)
            if 1 <= sec138_count <= 2:
                return 50.0
            if total_active == 1 and sec138_count == 0:
                return 80.0
            return 100.0
        except Exception as exc:
            logger.error("HealthScorer legal_risk failed for %s: %s", cin, exc)
            return 50.0

    async def _compute_financial_health(self, cin: str) -> float:
        try:
            row = await self.db.fetchrow(
                """
                SELECT debt_to_equity
                FROM financial_snapshots
                WHERE cin = $1
                ORDER BY financial_year DESC
                LIMIT 1
                """,
                cin,
            )

            if row is None or row["debt_to_equity"] is None:
                return 50.0

            debt_to_equity = float(row["debt_to_equity"])
            if debt_to_equity < 1.0:
                return 100.0
            if debt_to_equity <= 2.0:
                return 70.0
            if debt_to_equity <= 4.0:
                return 40.0
            return 15.0
        except Exception as exc:
            logger.error("HealthScorer financial_health failed for %s: %s", cin, exc)
            return 50.0

    async def _compute_capital_trajectory(self, cin: str) -> float:
        try:
            current_row = await self.db.fetchrow(
                "SELECT paid_up_capital FROM master_entities WHERE cin = $1",
                cin,
            )
            if current_row is None or current_row["paid_up_capital"] is None:
                return 50.0

            current_capital = int(current_row["paid_up_capital"])

            prev_row = await self.db.fetchrow(
                """
                SELECT data_json->>'previous_paid_up_capital' AS prev_capital
                FROM events
                WHERE cin = $1
                  AND event_type IN ('CAPITAL_CHANGE')
                  AND data_json ? 'previous_paid_up_capital'
                ORDER BY detected_at DESC
                LIMIT 1
                """,
                cin,
            )

            if prev_row is None or prev_row["prev_capital"] is None:
                return 50.0

            try:
                previous_capital = int(prev_row["prev_capital"])
            except (TypeError, ValueError):
                return 50.0

            if previous_capital == 0 or current_capital == 0:
                return 50.0

            pct_change = ((current_capital - previous_capital) / previous_capital) * 100
            if pct_change > 5:
                return 100.0
            if pct_change >= -5:
                return 60.0
            return 20.0
        except Exception as exc:
            logger.error("HealthScorer capital_trajectory failed for %s: %s", cin, exc)
            return 50.0

    async def _apply_contagion_penalty(
        self, cin: str, raw_score: float
    ) -> tuple[float, float, list[str]]:
        try:
            din_rows = await self.db.fetch(
                "SELECT din FROM governance_graph WHERE cin = $1 AND is_active = TRUE",
                cin,
            )
            if not din_rows:
                return (max(0.0, min(100.0, raw_score)), 0.0, [])

            dins = [row["din"] for row in din_rows]
            peer_rows = await self.db.fetch(
                """
                SELECT DISTINCT gg.cin, me.health_band
                FROM governance_graph gg
                JOIN master_entities me ON me.cin = gg.cin
                WHERE gg.din = ANY($1::varchar[])
                  AND gg.is_active = TRUE
                  AND gg.cin != $2
                """,
                dins,
                cin,
            )

            penalty = 0.0
            contributing_cins: list[str] = []
            seen: set[str] = set()

            for row in peer_rows:
                peer_cin = row["cin"]
                if peer_cin in seen:
                    continue
                seen.add(peer_cin)

                if row["health_band"] == "RED":
                    penalty += 15.0
                    contributing_cins.append(peer_cin)
                elif row["health_band"] == "AMBER":
                    penalty += 5.0
                    contributing_cins.append(peer_cin)

            final_score = max(0.0, min(100.0, raw_score - penalty))
            return (final_score, penalty, contributing_cins)
        except Exception as exc:
            logger.error("HealthScorer contagion failed for %s: %s", cin, exc)
            return (max(0.0, min(100.0, raw_score)), 0.0, [])

    async def _get_band(self, score: float) -> str:
        if score >= 70.0:
            return "GREEN"
        if score >= 40.0:
            return "AMBER"
        return "RED"

    async def _persist_score(
        self,
        cin: str,
        score: float,
        band: str,
        components: dict,
        event_id: int,
        previous_score: float,
        previous_band: str,
    ) -> None:
        async with self.db.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    """
                    UPDATE master_entities
                    SET health_score = $1,
                        health_band  = $2,
                        last_score_computed_at = NOW()
                    WHERE cin = $3
                    """,
                    int(score),
                    band,
                    cin,
                )

                await conn.execute(
                    """
                    UPDATE events
                    SET health_score_before = $1,
                        health_score_after  = $2
                    WHERE id = $3
                    """,
                    int(previous_score),
                    int(score),
                    event_id,
                )

                await conn.execute(
                    """
                    INSERT INTO predictions (cin, health_score_at_firing, severity, fired_at)
                    SELECT $1, $2, severity, NOW()
                    FROM events
                    WHERE id = $3
                    """,
                    cin,
                    int(score),
                    event_id,
                )


class ContagionPropagator:
    MAX_DEPTH = 2

    def __init__(self, db_pool: asyncpg.Pool, health_scorer: HealthScorer):
        self.db = db_pool
        self.scorer = health_scorer

    async def propagate(
        self,
        cin: str,
        new_band: str,
        depth: int = 0,
        _visited: Optional[set] = None,
    ) -> List[str]:
        if _visited is None:
            _visited = set()
        if depth >= self.MAX_DEPTH:
            return []

        _visited.add(cin)
        rescored: List[str] = []

        try:
            din_rows = await self.db.fetch(
                "SELECT din FROM governance_graph WHERE cin = $1 AND is_active = TRUE",
                cin,
            )
            if not din_rows:
                return []

            dins = [row["din"] for row in din_rows]
            peer_rows = await self.db.fetch(
                """
                SELECT DISTINCT cin
                FROM governance_graph
                WHERE din = ANY($1::varchar[])
                  AND is_active = TRUE
                  AND cin != $2
                """,
                dins,
                cin,
            )
            peer_cins = [row["cin"] for row in peer_rows if row["cin"] not in _visited]

            for peer_cin in peer_cins:
                if peer_cin in _visited:
                    continue

                try:
                    _visited.add(peer_cin)
                    payload = {
                        "origin_cin": cin,
                        "origin_new_band": new_band,
                        "depth": depth,
                        "chain": sorted(_visited),
                    }
                    contagion_event_id = await self.db.fetchval(
                        """
                        INSERT INTO events (cin, source, event_type, severity, detected_at,
                                            data_json, contagion_checked)
                        VALUES ($1, 'CONTAGION_ENGINE', 'CONTAGION_PROPAGATED', 'WATCH',
                                NOW(), $2::jsonb, TRUE)
                        RETURNING id
                        """,
                        peer_cin,
                        json.dumps(payload),
                    )

                    result = await self.scorer.recompute(peer_cin, contagion_event_id)
                    rescored.append(peer_cin)

                    if result.band != result.previous_band:
                        rescored.extend(
                            await self.propagate(
                                peer_cin,
                                result.band,
                                depth=depth + 1,
                                _visited=_visited,
                            )
                        )

                    try:
                        await self.db.execute(
                            """
                            UPDATE events
                            SET contagion_chain = $1::jsonb
                            WHERE id = $2
                            """,
                            json.dumps({"rescored_cins": rescored, "depth": depth}),
                            contagion_event_id,
                        )
                    except Exception as exc:
                        logger.warning("Failed to update contagion_chain: %s", exc)
                except Exception as exc:
                    logger.error("Contagion propagation failed for peer %s: %s", peer_cin, exc)

            return list(dict.fromkeys(rescored))
        except Exception as exc:
            logger.error("Contagion propagation failed for %s: %s", cin, exc)
            return list(dict.fromkeys(rescored))
