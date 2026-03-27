"""
Pydantic request and response models for all /op/ endpoints.

All models use Pydantic v2 (model_config, not class Config).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class SourceStatusItem(BaseModel):
    source_id: str
    status: str
    last_pull_at: Optional[datetime]
    record_count: Optional[int]
    consecutive_failures: int
    next_pull_at: Optional[datetime]
    lag_hours: Optional[float]


class EventItem(BaseModel):
    id: int
    cin: Optional[str]
    company_name: Optional[str] = None
    source: str
    event_type: str
    severity: str
    detected_at: datetime
    health_score_before: Optional[int]
    health_score_after: Optional[int]
    data_json_summary: str


class HealthComponent(BaseModel):
    raw: float
    weighted: float


class HealthComponents(BaseModel):
    filing_freshness: HealthComponent
    director_stability: HealthComponent
    legal_risk: HealthComponent
    financial_health: HealthComponent
    capital_trajectory: HealthComponent


class DirectorItem(BaseModel):
    din: str
    director_name: Optional[str]
    designation: Optional[str]
    date_of_appointment: Optional[Any]
    is_active: bool


class LegalCaseItem(BaseModel):
    id: int
    case_type: str
    case_number: Optional[str]
    court: Optional[str]
    filing_date: Optional[Any]
    status: Optional[str]
    amount_involved: Optional[int]


class CompanyHealthResponse(BaseModel):
    cin: str
    company_name: str
    health_score: int
    health_band: Optional[str]
    last_computed_at: Optional[datetime]
    components: HealthComponents
    recent_events: list[EventItem]
    active_legal_cases: list[LegalCaseItem]
    directors: list[DirectorItem]


class SourceLagItem(BaseModel):
    source_id: str
    expected_next_pull: Optional[datetime]
    actual_lag_hours: float
    status: str


class ScraperHealthItem(BaseModel):
    source_id: str
    status: str
    color: Literal["green", "amber", "red", "grey"]
    last_pull_at: Optional[datetime]
    record_count: Optional[int]
    consecutive_failures: int
    lag_hours: Optional[float]
    note: str


class ScraperHealthSummary(BaseModel):
    as_of: datetime
    total_scrapers: int
    green: int
    amber: int
    red: int
    grey: int
    unmapped_signals: int
    events_today: int
    alerts_delivered_today: int
    scrapers: list[ScraperHealthItem]


class FalsePositiveCause(BaseModel):
    event_type: str
    count: int


class AccuracyResponse(BaseModel):
    window_days: int
    total_red_alerts: int
    confirmed: int
    false_positives: int
    expired_unconfirmed: int
    accuracy_pct: float
    top_false_positive_causes: list[FalsePositiveCause]


class CostBreakdownItem(BaseModel):
    service: str
    operation: Optional[str]
    units: Optional[int]
    cost_inr: float


class CostsTodayResponse(BaseModel):
    date: str
    total_inr: float
    breakdown: list[CostBreakdownItem]
    alert_threshold_inr: float
    threshold_breached: bool


class WatchlistCreateRequest(BaseModel):
    name: str = Field(..., min_length=1)
    cin_list: Optional[list[str]] = None
    state_filter: Optional[str] = None
    sector_filter: Optional[str] = None
    severity_min: Optional[str] = "WATCH"
    signal_types: Optional[list[str]] = None


class WatchlistCreateResponse(BaseModel):
    id: int
    name: str
    cin_list: Optional[list[str]]
    state_filter: Optional[str]
    sector_filter: Optional[str]
    severity_min: str
    signal_types: Optional[list[str]]
    is_active: bool
    created_at: datetime


class SubscriberItem(BaseModel):
    id: int
    name: str
    email: str
    severity_threshold: Literal["WATCH", "AMBER", "RED", "CRITICAL"]
    watchlist_count: int
    alert_count_this_month: int


class SubscriberCreateRequest(BaseModel):
    name: str = Field(..., min_length=1)
    email: str = Field(..., min_length=3)
    severity_threshold: Literal["WATCH", "AMBER", "RED", "CRITICAL"]


class SubscriberCreateResponse(BaseModel):
    id: int
    name: str
    email: str
    severity_threshold: Literal["WATCH", "AMBER", "RED", "CRITICAL"]


class SubscriberWatchlistCreateRequest(BaseModel):
    cin: str = Field(..., pattern=r"^[UL]\d{5}[A-Z]{2}\d{4}[A-Z]{3}\d{6}$")


class SubscriberWatchlistItem(BaseModel):
    cin: str
    company_name: str
    added_at: datetime


class CompanyEventResponseItem(BaseModel):
    event_date: datetime
    event_type: str
    severity: str
    source: str
    notes: str


class CompanyHealthSummaryResponse(BaseModel):
    cin: str
    health_score: int
    band: Optional[str]
    last_computed: Optional[datetime]


class EnrichResponse(BaseModel):
    cin: str
    status: str
    message: str


class ResolveRequest(BaseModel):
    queue_id: int
    resolved_cin: str


class ResolveResponse(BaseModel):
    queue_id: int
    resolved_cin: str
    message: str


class RecalibrateResponse(BaseModel):
    status: str
    message: str


class CaptchaSolveRequest(BaseModel):
    source_id: str
    solution: str


class CaptchaSolveResponse(BaseModel):
    status: str
    source_id: str


class AlertFeedbackRequest(BaseModel):
    action: Literal["confirm", "false_positive"]
    reason: Optional[str] = None


class AlertFeedbackResponse(BaseModel):
    prediction_id: int
    action: str
    message: str


class RecalibrateResult(BaseModel):
    sources_adjusted: int
    thresholds_raised: int
    thresholds_lowered: int
    predictions_expired: int
    summary: str
