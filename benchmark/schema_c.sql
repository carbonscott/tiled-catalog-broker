-- ============================================================
-- Schema C — Hierarchical + JSONB  (current MAIQMag design)
-- ============================================================

DROP TABLE IF EXISTS c_artifacts CASCADE;
DROP TABLE IF EXISTS c_entities CASCADE;
DROP TABLE IF EXISTS c_datasets CASCADE;

CREATE TABLE c_datasets (
    id    SERIAL PRIMARY KEY,
    name  TEXT NOT NULL,
    meta  JSONB   -- dtype, material, producer, facility, instrument, layout,
                  -- params_location, file_format, size_gb, n_files
);

CREATE TABLE c_entities (
    id          SERIAL PRIMARY KEY,
    dataset_id  INT REFERENCES c_datasets(id),
    name        TEXT NOT NULL,
    meta        JSONB   -- all params for this entity (sparse — only keys present)
);

CREATE TABLE c_artifacts (
    id           SERIAL PRIMARY KEY,
    entity_id    INT REFERENCES c_entities(id),
    name         TEXT NOT NULL,
    array_shape  INT[],
    shared_axes  TEXT[],
    data_ref     TEXT
);

-- GIN indexes on JSONB
CREATE INDEX idx_c_datasets_meta         ON c_datasets  USING GIN (meta);
CREATE INDEX idx_c_entities_meta         ON c_entities  USING GIN (meta);

-- FK B-tree indexes
CREATE INDEX idx_c_entities_dataset_id   ON c_entities(dataset_id);
CREATE INDEX idx_c_artifacts_entity_id   ON c_artifacts(entity_id);

-- Expression indexes for the most frequently queried scalar params
CREATE INDEX idx_c_entities_Ja_mev       ON c_entities ((meta->>'Ja_mev'));
CREATE INDEX idx_c_entities_spin_s       ON c_entities ((meta->>'spin_s'));
CREATE INDEX idx_c_entities_twotheta     ON c_entities ((meta->>'twotheta_deg'));
CREATE INDEX idx_c_entities_temperature  ON c_entities ((meta->>'temperature_K'));
CREATE INDEX idx_c_entities_Udd          ON c_entities ((meta->>'Udd'));
CREATE INDEX idx_c_datasets_material     ON c_datasets  ((meta->>'material'));
CREATE INDEX idx_c_datasets_dtype        ON c_datasets  ((meta->>'dtype'));
CREATE INDEX idx_c_datasets_layout       ON c_datasets  ((meta->>'layout'));
