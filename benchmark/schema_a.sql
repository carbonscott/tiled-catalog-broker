-- ============================================================
-- Schema A — Hierarchical + Native Columns
-- 6 datasets: VDP, NiPS3_multimodal, EDRIXS_Sam, EDRIXS_Tlinker,
--             SUNNY_10K, SUNNY_EXP_mesh
-- ============================================================

DROP TABLE IF EXISTS a_artifacts CASCADE;
DROP TABLE IF EXISTS a_entities CASCADE;
DROP TABLE IF EXISTS a_datasets CASCADE;

CREATE TABLE a_datasets (
    id               SERIAL PRIMARY KEY,
    name             TEXT NOT NULL,
    dtype            TEXT,           -- 'simulation' | 'experimental'
    material         TEXT,
    producer         TEXT,
    facility         TEXT,
    instrument       TEXT,
    layout           TEXT,           -- 'per-entity' | 'batched' | 'monolithic' | 'per-entity-in-groups'
    params_location  TEXT,           -- where params live in the file
    file_format      TEXT,           -- 'HDF5'
    size_gb          FLOAT
);

-- Superset of all param keys across the 6 datasets. NULL where not applicable.
CREATE TABLE a_entities (
    id                    SERIAL PRIMARY KEY,
    dataset_id            INT REFERENCES a_datasets(id),
    name                  TEXT NOT NULL,
    -- Shared: VDP / NiPS3 / SUNNY
    Ja_mev                FLOAT,
    Jb_mev                FLOAT,
    Jc_mev                FLOAT,
    D_mev                 FLOAT,
    Gamma_mev             FLOAT,
    spin_s                FLOAT,
    g_factor              FLOAT,
    material_param        TEXT,
    temperature_K         FLOAT,
    field_T               FLOAT,
    broadening_mev        FLOAT,
    -- VDP specific
    H_T_max               FLOAT,
    q_Ainv_max            FLOAT,
    hw_meV_max            FLOAT,
    -- EDRIXS (Sam + Tlinker)
    Udd                   FLOAT,
    Upd                   FLOAT,
    Delta                 FLOAT,
    crystal_10Dq          FLOAT,
    zeta_d                FLOAT,
    zeta_p                FLOAT,
    Ds                    FLOAT,
    Dt                    FLOAT,
    incident_energy_eV    FLOAT,
    eloss_min_eV          FLOAT,
    eloss_max_eV          FLOAT
);

CREATE TABLE a_artifacts (
    id           SERIAL PRIMARY KEY,
    entity_id    INT REFERENCES a_entities(id),
    name         TEXT NOT NULL,
    array_shape  INT[],
    shared_axes  TEXT[],
    data_ref     TEXT
);

-- Indexes
CREATE INDEX idx_a_entities_dataset_id  ON a_entities(dataset_id);
CREATE INDEX idx_a_artifacts_entity_id  ON a_artifacts(entity_id);
CREATE INDEX idx_a_datasets_dtype       ON a_datasets(dtype);
CREATE INDEX idx_a_datasets_material    ON a_datasets(material);
CREATE INDEX idx_a_datasets_layout      ON a_datasets(layout);
CREATE INDEX idx_a_entities_Ja_mev      ON a_entities(Ja_mev);
CREATE INDEX idx_a_entities_spin_s      ON a_entities(spin_s);
CREATE INDEX idx_a_entities_temperature ON a_entities(temperature_K);
CREATE INDEX idx_a_entities_Udd         ON a_entities(Udd);
