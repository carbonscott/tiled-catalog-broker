-- ============================================================
-- Schema D — Flat + JSONB
-- ============================================================

DROP TABLE IF EXISTS d_flat CASCADE;

CREATE TABLE d_flat (
    id              SERIAL PRIMARY KEY,
    dataset_name    TEXT,
    dataset_meta    JSONB,   -- dtype, material, layout, file_format, size_gb, ...
    entity_name     TEXT,
    entity_meta     JSONB,   -- sparse params for this entity
    artifact_name   TEXT,
    array_shape     INT[],
    shared_axes     TEXT[],
    data_ref        TEXT
);

-- GIN indexes on JSONB
CREATE INDEX idx_d_dataset_meta          ON d_flat USING GIN (dataset_meta);
CREATE INDEX idx_d_entity_meta           ON d_flat USING GIN (entity_meta);

-- B-tree helpers
CREATE INDEX idx_d_dataset_name          ON d_flat(dataset_name);
CREATE INDEX idx_d_entity_name           ON d_flat(entity_name);

-- Expression indexes
CREATE INDEX idx_d_entity_Ja_mev         ON d_flat ((entity_meta->>'Ja_mev'));
CREATE INDEX idx_d_entity_spin_s         ON d_flat ((entity_meta->>'spin_s'));
CREATE INDEX idx_d_entity_twotheta       ON d_flat ((entity_meta->>'twotheta_deg'));
CREATE INDEX idx_d_entity_temperature    ON d_flat ((entity_meta->>'temperature_K'));
CREATE INDEX idx_d_entity_Udd            ON d_flat ((entity_meta->>'Udd'));
CREATE INDEX idx_d_dataset_material      ON d_flat ((dataset_meta->>'material'));
CREATE INDEX idx_d_dataset_dtype         ON d_flat ((dataset_meta->>'dtype'));
CREATE INDEX idx_d_dataset_layout        ON d_flat ((dataset_meta->>'layout'));
