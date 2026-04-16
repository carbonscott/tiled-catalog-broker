-- ============================================================
-- Schema B — Flat + Native Columns
-- ============================================================

DROP TABLE IF EXISTS b_flat CASCADE;

CREATE TABLE b_flat (
    id                    SERIAL PRIMARY KEY,
    -- Dataset-level
    dataset_name          TEXT,
    dtype                 TEXT,
    material              TEXT,
    producer              TEXT,
    facility              TEXT,
    instrument            TEXT,
    layout                TEXT,
    params_location       TEXT,
    file_format           TEXT,
    size_gb               FLOAT,
    -- Entity
    entity_name           TEXT,
    -- Params superset
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
    H_T_max               FLOAT,
    q_Ainv_max            FLOAT,
    hw_meV_max            FLOAT,
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
    eloss_max_eV          FLOAT,
    -- Artifact
    artifact_name         TEXT,
    array_shape           INT[],
    shared_axes           TEXT[],
    data_ref              TEXT
);

-- Indexes
CREATE INDEX idx_b_dtype           ON b_flat(dtype);
CREATE INDEX idx_b_material        ON b_flat(material);
CREATE INDEX idx_b_layout          ON b_flat(layout);
CREATE INDEX idx_b_dataset_name    ON b_flat(dataset_name);
CREATE INDEX idx_b_entity_name     ON b_flat(entity_name);
CREATE INDEX idx_b_Ja_mev          ON b_flat(Ja_mev);
CREATE INDEX idx_b_spin_s          ON b_flat(spin_s);
CREATE INDEX idx_b_temperature     ON b_flat(temperature_K);
CREATE INDEX idx_b_Udd             ON b_flat(Udd);
