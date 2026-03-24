# SCHEMA_SPEC.md

## Overview

This file defines the complete PostgreSQL schema for the signal intelligence network. The system monitors 35 Indian government sources for corporate distress events across 18 lakh+ companies. Every fragment of data resolves to a CIN (Corporate Identity Number) before entering the main tables. The schema is organized around three pipeline layers: ingestion (`source_state`), detection (`events`, `legal_events`, `financial_snapshots`), and routing (`alerts`, `watchlists`). Entity resolution is handled offline via `entity_resolution_queue` and `unmapped_signals` before promotion to `master_entities`.

---

## Complete DDL

```sql
-- =============================================================================
-- EXTENSIONS
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS pg_trgm;


-- =============================================================================
-- UTILITY: updated_at trigger function
-- Applied to any table that carries an updated_at column.
-- =============================================================================

CREATE OR REPLACE FUNCTION trigger_set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;


-- =============================================================================
-- TABLE: master_entities
-- The golden record. One row per company. CIN is the only primary key.
-- =============================================================================

CREATE TABLE master_entities (
  cin                     VARCHAR(21)   PRIMARY KEY,
  company_name            TEXT          NOT NULL,
  normalized_name         TEXT,
  status                  VARCHAR(20)   NOT NULL
                            CHECK (status IN (
                              'Active',
                              'Struck Off',
                              'Under Liquidation',
                              'Converted to LLP',
                              'Dissolved'
                            )),
  registered_state        VARCHAR(5),
  industrial_class        VARCHAR(10),
  date_of_incorporation   DATE,
  date_of_last_agm        DATE,
  authorized_capital      BIGINT,
  paid_up_capital         BIGINT,
  company_category        VARCHAR(50),
  company_subcategory     VARCHAR(50),
  registered_address      TEXT,
  email                   VARCHAR(255),
  pan                     VARCHAR(10),
  health_score            SMALLINT      NOT NULL DEFAULT 50
                            CHECK (health_score BETWEEN 0 AND 100),
  health_band             VARCHAR(10)
                            CHECK (health_band IN ('GREEN', 'AMBER', 'RED', 'WATCH')),
  last_score_computed_at  TIMESTAMP,
  created_at              TIMESTAMP     NOT NULL DEFAULT NOW(),
  updated_at              TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_master_entities_name_trgm
  ON master_entities USING GIN (company_name gin_trgm_ops);

CREATE INDEX idx_master_entities_normalized_name_trgm
  ON master_entities USING GIN (normalized_name gin_trgm_ops);

CREATE INDEX idx_master_entities_status
  ON master_entities (status);

CREATE INDEX idx_master_entities_registered_state
  ON master_entities (registered_state);

CREATE INDEX idx_master_entities_industrial_class
  ON master_entities (industrial_class);

CREATE INDEX idx_master_entities_health_band
  ON master_entities (health_band);

CREATE INDEX idx_master_entities_last_agm_active
  ON master_entities (date_of_last_agm)
  WHERE status = 'Active';

CREATE TRIGGER trg_master_entities_updated_at
  BEFORE UPDATE ON master_entities
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();


-- =============================================================================
-- TABLE: governance_graph
-- Director-to-company mapping. Every DIN-CIN relationship tracked here.
-- =============================================================================

CREATE TABLE governance_graph (
  id                    SERIAL        PRIMARY KEY,
  din                   VARCHAR(10)   NOT NULL,
  cin                   VARCHAR(21)   NOT NULL REFERENCES master_entities (cin),
  director_name         TEXT,
  designation           VARCHAR(100),
  date_of_appointment   DATE,
  cessation_date        DATE,
  is_active             BOOLEAN       NOT NULL DEFAULT TRUE,
  source                VARCHAR(50),
  created_at            TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_governance_graph_din_active
  ON governance_graph (din)
  WHERE is_active = TRUE;

CREATE INDEX idx_governance_graph_cin
  ON governance_graph (cin);

CREATE UNIQUE INDEX idx_governance_graph_din_cin_appointment
  ON governance_graph (din, cin, date_of_appointment);


-- =============================================================================
-- TABLE: source_state
-- One row per monitored source. Tracks last pull and hash for diff engine.
-- =============================================================================

CREATE TABLE source_state (
  source_id             VARCHAR(100)  PRIMARY KEY,
  last_pull_at          TIMESTAMP,
  last_data_hash        VARCHAR(64),
  record_count          INTEGER,
  status                VARCHAR(20)   NOT NULL DEFAULT 'OK'
                          CHECK (status IN (
                            'OK',
                            'DEGRADED',
                            'UNREACHABLE',
                            'SCRAPER_BROKEN'
                          )),
  consecutive_failures  SMALLINT      NOT NULL DEFAULT 0,
  next_pull_at          TIMESTAMP,
  notes                 TEXT,
  updated_at            TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_source_state_updated_at
  BEFORE UPDATE ON source_state
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();


-- =============================================================================
-- TABLE: events
-- Every detected change. Core of the detection layer.
-- =============================================================================

CREATE TABLE events (
  id                    BIGSERIAL     PRIMARY KEY,
  cin                   VARCHAR(21)   REFERENCES master_entities (cin),
  source                VARCHAR(50)   NOT NULL,
  event_type            VARCHAR(50)   NOT NULL,
  severity              VARCHAR(10)   NOT NULL
                          CHECK (severity IN ('INFO', 'WATCH', 'ALERT', 'CRITICAL')),
  detected_at           TIMESTAMP     NOT NULL DEFAULT NOW(),
  data_json             JSONB         NOT NULL,
  health_score_before   SMALLINT,
  health_score_after    SMALLINT,
  contagion_checked     BOOLEAN       NOT NULL DEFAULT FALSE,
  contagion_chain       JSONB,
  is_deduplicated       BOOLEAN       NOT NULL DEFAULT FALSE,
  batch_id              VARCHAR(36)
);

CREATE INDEX idx_events_cin_detected
  ON events (cin, detected_at DESC);

CREATE INDEX idx_events_event_type_detected
  ON events (event_type, detected_at DESC);

CREATE INDEX idx_events_severity_detected
  ON events (severity, detected_at DESC);

CREATE INDEX idx_events_detected_at
  ON events (detected_at DESC);

CREATE INDEX idx_events_data_json
  ON events USING GIN (data_json);


-- =============================================================================
-- TABLE: watchlists
-- Subscriber-defined alert filters controlling which events generate alerts.
-- =============================================================================

CREATE TABLE watchlists (
  id                  SERIAL        PRIMARY KEY,
  name                TEXT          NOT NULL,
  subscriber_id       INTEGER,
  cin_list            VARCHAR(21)[],
  state_filter        VARCHAR(5),
  sector_filter       VARCHAR(10),
  severity_min        VARCHAR(10)   NOT NULL DEFAULT 'WATCH'
                        CHECK (severity_min IN ('INFO', 'WATCH', 'ALERT', 'CRITICAL')),
  signal_types        TEXT[],
  is_active           BOOLEAN       NOT NULL DEFAULT TRUE,
  backfill_enabled    BOOLEAN       NOT NULL DEFAULT FALSE,
  created_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE TRIGGER trg_watchlists_updated_at
  BEFORE UPDATE ON watchlists
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();


-- =============================================================================
-- TABLE: alerts
-- Every alert generated from an event matching a watchlist.
-- =============================================================================

CREATE TABLE alerts (
  id                  BIGSERIAL     PRIMARY KEY,
  event_id            BIGINT        REFERENCES events (id),
  watchlist_id        INTEGER       REFERENCES watchlists (id),
  cin                 VARCHAR(21),
  severity            VARCHAR(10),
  alert_payload       JSONB,
  ai_summary          TEXT,
  delivered_at        TIMESTAMP,
  delivery_channel    VARCHAR(20),
  delivery_status     VARCHAR(20)   NOT NULL DEFAULT 'PENDING',
  batch_id            VARCHAR(36),
  created_at          TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_alerts_watchlist_delivered
  ON alerts (watchlist_id, delivered_at DESC);

CREATE INDEX idx_alerts_cin_created
  ON alerts (cin, created_at DESC);

CREATE INDEX idx_alerts_delivery_status_pending
  ON alerts (delivery_status)
  WHERE delivery_status != 'DELIVERED';


-- =============================================================================
-- TABLE: entity_resolution_queue
-- Unresolved or ambiguous name-to-CIN matches awaiting algorithm or LLM pass.
-- =============================================================================

CREATE TABLE entity_resolution_queue (
  id                  SERIAL        PRIMARY KEY,
  source              VARCHAR(50),
  raw_name            TEXT          NOT NULL,
  raw_identifier      TEXT,
  identifier_type     VARCHAR(20),
  candidates          JSONB,
  best_confidence     NUMERIC(5,2),
  resolution_method   VARCHAR(30),
  resolved_cin        VARCHAR(21),
  resolved            BOOLEAN       NOT NULL DEFAULT FALSE,
  llm_used            BOOLEAN       NOT NULL DEFAULT FALSE,
  operator_reviewed   BOOLEAN       NOT NULL DEFAULT FALSE,
  created_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
  resolved_at         TIMESTAMP
);

CREATE INDEX idx_entity_resolution_queue_unresolved
  ON entity_resolution_queue (resolved)
  WHERE resolved = FALSE;

CREATE INDEX idx_entity_resolution_queue_confidence_unresolved
  ON entity_resolution_queue (best_confidence)
  WHERE resolved = FALSE;


-- =============================================================================
-- TABLE: identifier_map
-- CIN to EPFO/GSTIN/PAN mappings built passively as signals are processed.
-- =============================================================================

CREATE TABLE identifier_map (
  cin                 VARCHAR(21)   NOT NULL REFERENCES master_entities (cin),
  identifier_type     VARCHAR(20)   NOT NULL,
  identifier_value    VARCHAR(50)   NOT NULL,
  source              VARCHAR(50),
  confidence          NUMERIC(5,2),
  created_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
  PRIMARY KEY (identifier_type, identifier_value)
);

CREATE INDEX idx_identifier_map_cin
  ON identifier_map (cin);


-- =============================================================================
-- TABLE: unmapped_signals
-- Signals that arrived but could not be resolved to a CIN yet.
-- =============================================================================

CREATE TABLE unmapped_signals (
  id                  SERIAL        PRIMARY KEY,
  source              VARCHAR(50),
  identifier_type     VARCHAR(20),
  identifier_value    VARCHAR(50),
  raw_data            JSONB,
  detected_at         TIMESTAMP     NOT NULL DEFAULT NOW(),
  resolved            BOOLEAN       NOT NULL DEFAULT FALSE,
  resolved_cin        VARCHAR(21),
  resolved_at         TIMESTAMP
);

CREATE INDEX idx_unmapped_signals_unresolved
  ON unmapped_signals (resolved)
  WHERE resolved = FALSE;

CREATE INDEX idx_unmapped_signals_identifier_type
  ON unmapped_signals (identifier_type);


-- =============================================================================
-- TABLE: financial_snapshots
-- Point-in-time financial data per company per filing year.
-- =============================================================================

CREATE TABLE financial_snapshots (
  id                  SERIAL        PRIMARY KEY,
  cin                 VARCHAR(21)   NOT NULL REFERENCES master_entities (cin),
  financial_year      VARCHAR(7)    NOT NULL,
  revenue             BIGINT,
  total_debt          BIGINT,
  net_worth           BIGINT,
  debt_to_equity      NUMERIC(8,2),
  profit_after_tax    BIGINT,
  source              VARCHAR(50),
  data_json           JSONB,
  created_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_financial_snapshots_cin_year_source
    UNIQUE (cin, financial_year, source)
);

CREATE INDEX idx_financial_snapshots_cin
  ON financial_snapshots (cin);


-- =============================================================================
-- TABLE: legal_events
-- Denormalized legal case tracking for fast health score computation.
-- =============================================================================

CREATE TABLE legal_events (
  id                  SERIAL        PRIMARY KEY,
  cin                 VARCHAR(21)   NOT NULL REFERENCES master_entities (cin),
  case_type           VARCHAR(50)   NOT NULL
                        CHECK (case_type IN (
                          'SEC_138',
                          'NCLT_7',
                          'NCLT_9',
                          'NCLT_10',
                          'DRT',
                          'SARFAESI_13_2',
                          'SARFAESI_13_4',
                          'SARFAESI_AUCTION',
                          'HIGH_COURT',
                          'LABOUR'
                        )),
  case_number         TEXT,
  court               TEXT,
  filing_date         DATE,
  status              VARCHAR(30),
  amount_involved     BIGINT,
  source              VARCHAR(50),
  event_id            BIGINT        REFERENCES events (id),
  created_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
  updated_at          TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_legal_events_cin
  ON legal_events (cin);

CREATE INDEX idx_legal_events_cin_case_type
  ON legal_events (cin, case_type);

CREATE TRIGGER trg_legal_events_updated_at
  BEFORE UPDATE ON legal_events
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();


-- =============================================================================
-- TABLE: enrichment_cache
-- CompData or other paid API responses cached to avoid redundant re-hits.
-- =============================================================================

CREATE TABLE enrichment_cache (
  cin                 VARCHAR(21)   NOT NULL REFERENCES master_entities (cin),
  source              VARCHAR(50)   NOT NULL,
  data_json           JSONB         NOT NULL,
  fetched_at          TIMESTAMP     NOT NULL DEFAULT NOW(),
  expires_at          TIMESTAMP     NOT NULL,
  PRIMARY KEY (cin, source)
);

CREATE INDEX idx_enrichment_cache_expires_at
  ON enrichment_cache (expires_at);


-- =============================================================================
-- TABLE: predictions
-- Feedback loop. Tracks fired predictions vs confirmed outcomes for recalibration.
-- =============================================================================

CREATE TABLE predictions (
  id                        SERIAL        PRIMARY KEY,
  cin                       VARCHAR(21),
  event_combination         TEXT[],
  health_score_at_firing    SMALLINT,
  severity                  VARCHAR(20),
  fired_at                  TIMESTAMP,
  confirmed                 BOOLEAN,
  confirmed_at              TIMESTAMP,
  false_positive            BOOLEAN,
  false_positive_reason     TEXT,
  auto_confirmed            BOOLEAN       NOT NULL DEFAULT FALSE,
  expired                   BOOLEAN       NOT NULL DEFAULT FALSE
);

CREATE INDEX idx_predictions_cin_fired
  ON predictions (cin, fired_at DESC);

CREATE INDEX idx_predictions_unconfirmed
  ON predictions (confirmed)
  WHERE confirmed IS NULL AND expired = FALSE;


-- =============================================================================
-- TABLE: captcha_log
-- Every CAPTCHA solve attempt logged for cost and success rate tracking.
-- =============================================================================

CREATE TABLE captcha_log (
  id          SERIAL        PRIMARY KEY,
  source      VARCHAR(50),
  method      VARCHAR(20)
                CHECK (method IN ('OCR', '2CAPTCHA', 'MANUAL')),
  success     BOOLEAN,
  cost_inr    NUMERIC(6,4)  NOT NULL DEFAULT 0,
  solved_at   TIMESTAMP     NOT NULL DEFAULT NOW()
);


-- =============================================================================
-- TABLE: cost_log
-- Daily API cost tracking. Operator review triggers when day total > INR 500.
-- =============================================================================

CREATE TABLE cost_log (
  id          SERIAL        PRIMARY KEY,
  log_date    DATE          NOT NULL,
  service     VARCHAR(50)   NOT NULL,
  operation   VARCHAR(100),
  units       INTEGER,
  cost_inr    NUMERIC(10,4),
  metadata    JSONB,
  created_at  TIMESTAMP     NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cost_log_log_date
  ON cost_log (log_date);


-- =============================================================================
-- ROLES AND GRANTS
-- =============================================================================

-- reader: read-only access to all tables
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'reader') THEN
    CREATE ROLE reader NOLOGIN;
  END IF;
END$$;

GRANT SELECT ON ALL TABLES IN SCHEMA public TO reader;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT ON TABLES TO reader;

-- writer: read/write on all tables except watchlists and cost_log
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'writer') THEN
    CREATE ROLE writer NOLOGIN;
  END IF;
END$$;

GRANT SELECT, INSERT, UPDATE ON
  master_entities,
  governance_graph,
  source_state,
  events,
  alerts,
  entity_resolution_queue,
  identifier_map,
  unmapped_signals,
  financial_snapshots,
  legal_events,
  enrichment_cache,
  predictions,
  captcha_log
TO writer;

GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO writer;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
  GRANT USAGE, SELECT ON SEQUENCES TO writer;

-- admin: full access
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'admin') THEN
    CREATE ROLE admin NOLOGIN;
  END IF;
END$$;

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO admin;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO admin;
```

---

## Design Decisions

**CIN as the only cross-table key.** `master_entities.cin` is the single anchor for all joins. Foreign keys from `events`, `governance_graph`, `legal_events`, `financial_snapshots`, `identifier_map`, and `enrichment_cache` all reference it directly. Signals that cannot be resolved to a CIN land in `unmapped_signals` or `entity_resolution_queue` and are never written to main tables until resolution is confirmed.

**No FK from `alerts.cin`.** `alerts.cin` is denormalized and carries no FK constraint. Alerts are delivery receipts — if a company is later struck off or a CIN is corrected, historical alert records must remain intact without cascade issues. The `event_id` FK to `events` provides the authoritative link.

**`events.cin` is nullable.** An event may be detected from a source before entity resolution completes. In that case the event is written with `cin = NULL` and linked to an `unmapped_signals` row. A background worker fills `cin` once resolution succeeds.

**`financial_year` stored as VARCHAR(7).** The format `'2023-24'` matches MCA filing nomenclature exactly. A CHECK constraint was omitted deliberately — source variations (e.g., `'FY2024'` from third-party enrichment) are normalized at ingestion time by the scraper, not at the DB layer.

**`legal_events.case_type` enum enforced via CHECK.** The 10 case types map directly to health score weight factors defined in `SKILLS.md`. Any new case type requires a schema migration, which is intentional — it forces a corresponding update to the scoring algorithm before data enters the table.

**`source_state` has no FK to any source registry.** Source IDs are string constants defined in scraper modules and `SOURCES.md`. Keeping `source_state` FK-free allows new sources to be onboarded by deploying a scraper without a schema migration.

**`watchlists` and `cost_log` excluded from `writer` role.** Watchlist mutations are subscriber-driven operations that should go through the FastAPI `/op/` layer with explicit authentication, not raw DB writes. `cost_log` is append-only audit data — the writer role inserts cost entries via the application layer, but cannot modify or delete them. A separate INSERT-only grant can be issued to the cost-tracking service account if needed.

**`updated_at` trigger applied selectively.** Only tables that represent mutable state (`master_entities`, `source_state`, `watchlists`, `legal_events`) carry an `updated_at` trigger. Append-only tables (`events`, `alerts`, `cost_log`, `captcha_log`) intentionally omit it — their rows are never updated, only inserted.

**GIN trigram indexes on `master_entities`.** Entity resolution fuzzy matching (`pg_trgm`) against 18 lakh+ company names requires GIN, not B-tree. Both `company_name` and `normalized_name` are indexed separately because resolution queries may hit either column depending on the normalization step applied to the incoming raw name.

**`predictions` table drives recalibration, not scoring.** Health scores are computed on-event from `legal_events`, `events`, and `financial_snapshots`. `predictions` only records what was fired and whether it was confirmed — the feedback loop in Phase 4 reads this table to adjust severity thresholds, not the score formula directly.

**`enrichment_cache.expires_at` index for sweep jobs.** A nightly job scans `expires_at < NOW()` to evict stale CompData responses. The B-tree index on `expires_at` makes this a fast range scan rather than a full-table sweep.
