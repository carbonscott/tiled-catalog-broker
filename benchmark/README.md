# MAIQMag Storage Benchmark
## Hierarchical vs Flat × JSONB vs Native Columns

A complete 2×2 benchmark for comparing PostgreSQL data model and storage format choices
for a scientific data catalog built on Tiled.

---

## Quick Start

### 1. Install dependencies

```bash
pip install psycopg2-binary numpy pandas matplotlib seaborn jupyter
```

### 2. Set up PostgreSQL


```bash
# Set up environment
source /sdf/data/lcls/ds/prj/prjmaiqmag01/results/cfitussi/postgres/setup_postgres.sh

# Start PostgreSQL (if not already running)
pg_ctl -D $PG_DATA -l $PG_LOG/postgres.log start

# Verify it's running
pg_ctl -D $PG_DATA status

# Connect to psql server
PGPASSWORD=vdp_secret psql -U vdp_admin -h localhost -p 5433 -d postgres

```psql
# Create database 
CREATE DATABASE bench;

# Make sure the database exists
\l

```bash

# Export DSN :

export DSN="postgresql://vdp_admin:vdp_secret@localhost:5433/bench?gssencmode=disable"
```

### 3. Run the full benchmark

```bash
# Small scale first (fast, ~1 min)
python benchmark_runner.py --dsn "$DSN" --scales small --warmup 3 --runs 10

# All scales (can take 10–30 min at large scale)
python benchmark_runner.py --dsn "$DSN" --scales small medium large
```

### 4. Visualise results

```bash
jupyter notebook benchmark_analysis.ipynb
```

---

## File Structure

```
benchmark/
├── sql/
│   ├── schema_a.sql          # Hierarchical + Native columns
│   ├── schema_b.sql          # Flat + Native columns
│   ├── schema_c.sql          # Hierarchical + JSONB (current design)
│   └── schema_d.sql          # Flat + JSONB (control cell)
├── generate_data.py          # Synthetic data generator and DB populator
├── benchmark_runner.py       # Query runner, timing, EXPLAIN capture
├── benchmark_analysis.ipynb  # Jupyter notebook with all plots
├── simulate_json.py          # (your existing file — used for exploration)
└── results/                  # Created automatically
    ├── benchmark_results.csv
    ├── summary_statistics.csv
    ├── explain_plans/        # One .txt per (query, schema, scale)
    └── *.png                 # All charts exported by the notebook
```

---

## The 2×2 Matrix

|                  | Hierarchical (3 tables) | Flat (1 table) |
|------------------|------------------------|----------------|
| **Native cols**  | Schema A               | Schema B       |
| **JSONB**        | Schema C ← current     | Schema D       |


---

## Query Suite

| Query | Pattern | What it tests |
|-------|---------|---------------|
| Q1 | Entity filter (`ja_mev > 5.0`) | Single-level filter, index selectivity |
| Q2 | Cross-level filter (material + spin_s) | Join cost vs deduplication cost |
| Q3 | Aggregation (entity count per dataset) | COUNT vs COUNT DISTINCT at scale |
| Q4 | Artifact retrieval (single entity) | Flat schema's best case |
| Q5 | Dataset-level UPDATE (facility) | Write amplification |
| Q6 | JSONB range scan (twotheta) | GIN index + cast performance (C, D only) |
| Q7 | Insert new entity + 10 artifacts | Write overhead, row fanout |

---

## Data Scales

| Scale  | Entities | Artifacts/entity | Total rows (flat) |
|--------|----------|------------------|-------------------|
| Small  | 100      | 10               | ~6,000            |
| Medium | 1,000    | 10               | ~60,000           |
| Large  | 10,000   | 50               | ~3,000,000        |

---

## Benchmark Options

```
python benchmark_runner.py --help

  --dsn          PostgreSQL connection string (required)
  --scales       small medium large  (default: all three)
  --warmup       Warmup runs before timing (default: 3)
  --runs         Timed runs per query (default: 10)
  --output       CSV output path (default: results/benchmark_results.csv)
  --explain-dir  EXPLAIN plan directory (default: results/explain_plans)
```

```
python generate_data.py --help

  --dsn          PostgreSQL connection string (required)
  --entities     Total entities across all 6 datasets (default: 1000)
  --artifacts    Artifacts per entity (default: 10)
  --schemas      Which schemas to populate: A B C D (default: all)
```

---

## Interpreting Results

The notebook (`benchmark_analysis.ipynb`) produces 6 charts:

1. **Overview** — bar chart of all queries at each scale
2. **Scaling curves** — latency vs volume per query, with ±1σ bands
3. **Table sizes** — storage footprint and amplification ratios
4. **Axis decomposition** — join cost (A−B) vs JSONB cost (C−A) vs total delta (C−B)
5. **Heatmaps** — latency matrix per scale (colour-coded)
6. **Scan type distribution** — Index vs Seq scans per schema (pie charts)


---

## Environment Notes

- PostgreSQL 15+ required (GIN + jsonb_set features used)
- `VACUUM ANALYZE` is run automatically after insertion
- `SET max_parallel_workers_per_gather = 0` is set before each query
