"""
benchmark_runner.py
===================
Runs the full query suite (Q1–Q7) across all four schemas at configurable scales.
Captures wall-clock time, EXPLAIN ANALYZE output, and table sizes.
Results are written to results/benchmark_results.csv.

Usage:
    python benchmark_runner.py --dsn "postgresql://user:pass@localhost/bench" \
                               --scales small medium large \
                               --warmup 3 --runs 10 --output results/benchmark_results.csv
"""

import argparse
import csv
import json
import os
import statistics
import time
from dataclasses import dataclass, field, asdict
from typing import Any

import psycopg2
import psycopg2.extras

# ---------------------------------------------------------------------------
# Scale definitions
# ---------------------------------------------------------------------------
SCALES = {
    "small":  {"entities": 100,   "artifacts": 10},
    "medium": {"entities": 1000,  "artifacts": 10},
    "large":  {"entities": 10000, "artifacts": 50},
}

# ---------------------------------------------------------------------------
# Query definitions
# ---------------------------------------------------------------------------
# Each query is a dict with per-schema SQL strings.
# Params are injected via psycopg2 %s placeholders.

QUERIES = {
    # ------------------------------------------------------------------
    # Q1 — Entity-level filter: find entities where Ja_mev > 5.0
    # ------------------------------------------------------------------
    "Q1_entity_filter": {
        "description": "Find all entities where Ja_mev > 5.0",
        "A": """
            SELECT e.id, e.name, e.Ja_mev, d.name AS dataset
            FROM a_entities e
            JOIN a_datasets d ON d.id = e.dataset_id
            WHERE e.Ja_mev > 5.0
        """,
        "B": """
            SELECT DISTINCT entity_name, Ja_mev, dataset_name
            FROM b_flat
            WHERE Ja_mev > 5.0
        """,
        "C": """
            SELECT e.id, e.name, e.meta->>'Ja_mev' AS Ja_mev, d.name AS dataset
            FROM c_entities e
            JOIN c_datasets d ON d.id = e.dataset_id
            WHERE (e.meta->>'Ja_mev')::float > 5.0
        """,
        "D": """
            SELECT DISTINCT entity_name, entity_meta->>'Ja_mev' AS Ja_mev, dataset_name
            FROM d_flat
            WHERE (entity_meta->>'Ja_mev')::float > 5.0
        """,
    },

    # ------------------------------------------------------------------
    # Q2 — Cross-level filter: material=NiPS3 AND spin_s > 1.0
    # ------------------------------------------------------------------
    "Q2_cross_level_filter": {
        "description": "Find all entities from material NiPS3 with spin_s > 1.0",
        "A": """
            SELECT e.id, e.name, e.spin_s, d.material
            FROM a_entities e
            JOIN a_datasets d ON d.id = e.dataset_id
            WHERE d.material = 'NiPS3' AND e.spin_s > 1.0
        """,
        "B": """
            SELECT DISTINCT entity_name, spin_s, material
            FROM b_flat
            WHERE material = 'NiPS3' AND spin_s > 1.0
        """,
        "C": """
            SELECT e.id, e.name, e.meta->>'spin_s' AS spin_s, d.meta->>'material' AS material
            FROM c_entities e
            JOIN c_datasets d ON d.id = e.dataset_id
            WHERE d.meta->>'material' = 'NiPS3'
              AND (e.meta->>'spin_s')::float > 1.0
        """,
        "D": """
            SELECT DISTINCT entity_name,
                   entity_meta->>'spin_s'        AS spin_s,
                   dataset_meta->>'material'      AS material
            FROM d_flat
            WHERE dataset_meta->>'material' = 'NiPS3'
              AND (entity_meta->>'spin_s')::float > 1.0
        """,
    },

    # ------------------------------------------------------------------
    # Q3 — Aggregation: count entities per dataset
    # ------------------------------------------------------------------
    "Q3_aggregation": {
        "description": "Count the number of entities per dataset",
        "A": """
            SELECT d.name, COUNT(e.id) AS entity_count
            FROM a_datasets d
            LEFT JOIN a_entities e ON e.dataset_id = d.id
            GROUP BY d.name
            ORDER BY entity_count DESC
        """,
        "B": """
            SELECT dataset_name, COUNT(DISTINCT entity_name) AS entity_count
            FROM b_flat
            GROUP BY dataset_name
            ORDER BY entity_count DESC
        """,
        "C": """
            SELECT d.name, COUNT(e.id) AS entity_count
            FROM c_datasets d
            LEFT JOIN c_entities e ON e.dataset_id = d.id
            GROUP BY d.name
            ORDER BY entity_count DESC
        """,
        "D": """
            SELECT dataset_name, COUNT(DISTINCT entity_name) AS entity_count
            FROM d_flat
            GROUP BY dataset_name
            ORDER BY entity_count DESC
        """,
    },

    # ------------------------------------------------------------------
    # Q4 — Artifact retrieval for a single entity (flat's best case)
    # ------------------------------------------------------------------
    "Q4_artifact_retrieval": {
        "description": "Get all artifacts for a single entity (parameterized)",
        "param_query": {
            "A": "SELECT name FROM a_entities LIMIT 1",
            "B": "SELECT DISTINCT entity_name FROM b_flat LIMIT 1",
            "C": "SELECT name FROM c_entities LIMIT 1",
            "D": "SELECT DISTINCT entity_name FROM d_flat LIMIT 1",
        },
        "A": """
            SELECT ar.name, ar.array_shape, ar.data_ref
            FROM a_artifacts ar
            JOIN a_entities e ON e.id = ar.entity_id
            WHERE e.name = %s
        """,
        "B": """
            SELECT artifact_name, array_shape, data_ref
            FROM b_flat
            WHERE entity_name = %s
        """,
        "C": """
            SELECT ar.name, ar.array_shape, ar.data_ref
            FROM c_artifacts ar
            JOIN c_entities e ON e.id = ar.entity_id
            WHERE e.name = %s
        """,
        "D": """
            SELECT artifact_name, array_shape, data_ref
            FROM d_flat
            WHERE entity_name = %s
        """,
    },

    # ------------------------------------------------------------------
    # Q5 — Dataset-level update: change facility
    # ------------------------------------------------------------------
    "Q5_dataset_update": {
        "description": "Update facility for RIXS_experimental dataset (LCLS → LCLS-II)",
        "A": "UPDATE a_datasets SET facility = 'LCLS-II' WHERE name = 'RIXS_experimental'",
        "A_reset": "UPDATE a_datasets SET facility = 'LCLS' WHERE name = 'RIXS_experimental'",
        "B": "UPDATE b_flat SET facility = 'LCLS-II' WHERE dataset_name = 'RIXS_experimental'",
        "B_reset": "UPDATE b_flat SET facility = 'LCLS' WHERE dataset_name = 'RIXS_experimental'",
        "C": """UPDATE c_datasets SET meta = jsonb_set(meta, '{facility}', '"LCLS-II"')
                WHERE name = 'RIXS_experimental'""",
        "C_reset": """UPDATE c_datasets SET meta = jsonb_set(meta, '{facility}', '"LCLS"')
                      WHERE name = 'RIXS_experimental'""",
        "D": """UPDATE d_flat SET dataset_meta = jsonb_set(dataset_meta, '{facility}', '"LCLS-II"')
                WHERE dataset_name = 'RIXS_experimental'""",
        "D_reset": """UPDATE d_flat SET dataset_meta = jsonb_set(dataset_meta, '{facility}', '"LCLS"')
                      WHERE dataset_name = 'RIXS_experimental'""",
    },

    # ------------------------------------------------------------------
    # Q6 — Range scan on JSONB (schemas C and D only)
    # ------------------------------------------------------------------
    "Q6_jsonb_range_scan": {
        "description": "JSONB range scan: entities where twotheta between 45 and 135 (C, D only)",
        "A": None,  # Not applicable
        "B": None,  # Not applicable
        "C": """
            SELECT e.id, e.name, e.meta->>'twotheta' AS twotheta
            FROM c_entities e
            WHERE (e.meta->>'twotheta')::float BETWEEN 45 AND 135
        """,
        "D": """
            SELECT DISTINCT entity_name, entity_meta->>'twotheta' AS twotheta
            FROM d_flat
            WHERE (entity_meta->>'twotheta')::float BETWEEN 45 AND 135
        """,
    },

    # ------------------------------------------------------------------
    # Q7 — Insert a new entity with 10 artifacts
    # ------------------------------------------------------------------
    "Q7_insert": {
        "description": "Insert a new entity with 10 artifacts into VDP_simulation dataset",
        # SQL is handled procedurally in run_query() below
        "procedural": True,
    },
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@dataclass
class BenchResult:
    query: str
    schema: str
    scale: str
    description: str
    avg_ms: float
    stddev_ms: float
    min_ms: float
    max_ms: float
    rows_returned: int
    scan_type: str
    table_size_mb: float
    explain: str = field(repr=False)


def get_table_sizes(cur, schema: str) -> float:
    """Return total size in MB for all tables in a schema."""
    tables = {
        "A": ["a_datasets", "a_entities", "a_artifacts"],
        "B": ["b_flat"],
        "C": ["c_datasets", "c_entities", "c_artifacts"],
        "D": ["d_flat"],
    }[schema]
    total = 0.0
    for t in tables:
        cur.execute("SELECT pg_total_relation_size(%s)", (t,))
        total += cur.fetchone()[0]
    return round(total / (1024 * 1024), 3)


def extract_scan_type(explain_text: str) -> str:
    """Parse the top-level scan node type from EXPLAIN output."""
    for line in explain_text.splitlines():
        stripped = line.strip()
        for kw in ("Index Scan", "Bitmap Heap Scan", "Seq Scan", "Index Only Scan",
                   "Nested Loop", "Hash Join", "Merge Join", "Aggregate"):
            if kw in stripped:
                return kw
    return "unknown"


def run_timed(cur, sql: str, params=None, warmup: int = 3, runs: int = 10):
    """Execute SQL with warmup runs, return (timings_ms, rowcount)."""
    for _ in range(warmup):
        cur.execute(sql, params)
        cur.fetchall()

    times = []
    rows = 0
    for _ in range(runs):
        t0 = time.perf_counter()
        cur.execute(sql, params)
        result = cur.fetchall()
        elapsed = (time.perf_counter() - t0) * 1000
        times.append(elapsed)
        rows = len(result)
    return times, rows


def run_explain(cur, sql: str, params=None) -> str:
    explain_sql = f"EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT) {sql}"
    cur.execute(explain_sql, params)
    rows = cur.fetchall()
    return "\n".join(r[0] for r in rows)


def insert_entity_a(cur) -> None:
    import uuid, random
    uid = uuid.uuid4().hex[:8]
    cur.execute("SELECT id FROM a_datasets WHERE name = 'VDP' " \
    "AND dtype= 'simulation'")
    ds_id = cur.fetchone()[0]
    cur.execute(
        """INSERT INTO a_entities (dataset_id, name, Ja_mev, spin_s)
           VALUES (%s, %s, %s, %s) RETURNING id""",
        (ds_id, f"E_bench_{uid}", 7.5, 1.0),
    )
    ent_id = cur.fetchone()[0]
    for i in range(10):
        cur.execute(
            "INSERT INTO a_artifacts (entity_id, name, array_shape, data_ref) VALUES (%s,%s,%s,%s)",
            (ent_id, f"art_{i}", [128, 128], f"s3://bench/E_bench_{uid}/art_{i}.h5"),
        )
    # cleanup
    cur.execute("DELETE FROM a_artifacts WHERE entity_id = %s", (ent_id,))
    cur.execute("DELETE FROM a_entities WHERE id = %s", (ent_id,))


def insert_entity_b(cur) -> None:
    import uuid
    uid = uuid.uuid4().hex[:8]
    rows = []
    for i in range(10):
        rows.append(("VDP", "simulation", "NiPS3", "MAIQ_team", None, "VDP_solver",
                     f"E_bench_{uid}", 7.5, None, None, None, 1.0, 2.0,
                     None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None,
                     f"art_{i}", [128, 128], f"s3://bench/E_bench_{uid}/art_{i}.h5"))
    psycopg2.extras.execute_values(
        cur,
        """INSERT INTO b_flat
           (dataset_name,dtype,material,producer,facility,instrument,entity_name,
            Ja_mev,Jb_mev,Jc_mev,D_mev,spin_s,g_factor,temperature_K,field_T,
            broadening_mev,H_T_max, q_Ainv_max,Udd, Upd,Delta, crystal_10Dq, zeta_d,zeta_p,Ds,
            Dt, incident_energy_eV,eloss_min_eV,eloss_max_eV,
            artifact_name,array_shape,data_ref)
           VALUES %s""",
        rows,
    )
    cur.execute("DELETE FROM b_flat WHERE entity_name = %s", (f"E_bench_{uid}",))


def insert_entity_c(cur) -> None:
    import uuid, json
    uid = uuid.uuid4().hex[:8]
    cur.execute("SELECT id FROM c_datasets WHERE name = 'VDP' AND meta->>'dtype'= 'simulation'")
    ds_id = cur.fetchone()[0]
    cur.execute(
        "INSERT INTO c_entities (dataset_id, name, meta) VALUES (%s,%s,%s) RETURNING id",
        (ds_id, f"E_bench_{uid}", json.dumps({"Ja_mev": 7.5, "spin_s": 1.0})),
    )
    ent_id = cur.fetchone()[0]
    for i in range(10):
        cur.execute(
            "INSERT INTO c_artifacts (entity_id, name, array_shape, data_ref) VALUES (%s,%s,%s,%s)",
            (ent_id, f"art_{i}", [128, 128], f"s3://bench/E_bench_{uid}/art_{i}.h5"),
        )
    cur.execute("DELETE FROM c_artifacts WHERE entity_id = %s", (ent_id,))
    cur.execute("DELETE FROM c_entities WHERE id = %s", (ent_id,))


def insert_entity_d(cur) -> None:
    import uuid, json
    uid = uuid.uuid4().hex[:8]
    rows = []
    for i in range(10):
        rows.append((
            "VDP_simulation", json.dumps({"dtype": "simulation", "material": "NiPS3"}),
            f"E_bench_{uid}", json.dumps({"Ja_mev": 7.5, "spin_s": 1.0}),
            f"art_{i}", [128, 128], f"s3://bench/E_bench_{uid}/art_{i}.h5",
        ))
    psycopg2.extras.execute_values(
        cur,
        "INSERT INTO d_flat (dataset_name,dataset_meta,entity_name,entity_meta,artifact_name,array_shape,data_ref) VALUES %s",
        rows,
    )
    cur.execute("DELETE FROM d_flat WHERE entity_name = %s", (f"E_bench_{uid}",))


Q7_INSERTERS = {"A": insert_entity_a, "B": insert_entity_b, "C": insert_entity_c, "D": insert_entity_d}


# ---------------------------------------------------------------------------
# Main benchmark loop
# ---------------------------------------------------------------------------

def benchmark(dsn: str, scales: list[str], warmup: int, runs: int, output: str, explain_dir: str):
    os.makedirs(os.path.dirname(output) if os.path.dirname(output) else ".", exist_ok=True)
    os.makedirs(explain_dir, exist_ok=True)

    results: list[BenchResult] = []

    for scale in scales:
        scale_cfg = SCALES[scale]
        print(f"\n{'='*60}")
        print(f"  Scale: {scale}  ({scale_cfg['entities']} entities × {scale_cfg['artifacts']} artifacts)")
        print(f"{'='*60}")

        # Populate data
        from generate_data import run as populate
        populate(dsn, scale_cfg["entities"], scale_cfg["artifacts"], ["A","B","C","D"])

        conn = psycopg2.connect(dsn)
        conn.autocommit = False
        cur = conn.cursor()

        # Disable parallel workers for reproducibility
        cur.execute("SET max_parallel_workers_per_gather = 0")

        for qname, qdef in QUERIES.items():
            desc = qdef.get("description", qname)
            print(f"\n  Query {qname}: {desc}")

            for schema in ["A", "B", "C", "D"]:
                sql = qdef.get(schema)

                # Q6 only for C and D
                if qname == "Q6_jsonb_range_scan" and schema in ("A", "B"):
                    continue

                # Q7 procedural
                if qdef.get("procedural"):
                    inserter = Q7_INSERTERS[schema]
                    times = []
                    for _ in range(warmup):
                        inserter(cur)
                        conn.commit()
                    for _ in range(runs):
                        t0 = time.perf_counter()
                        inserter(cur)
                        conn.commit()
                        times.append((time.perf_counter() - t0) * 1000)
                    size_mb = get_table_sizes(cur, schema)
                    res = BenchResult(
                        query=qname, schema=schema, scale=scale, description=desc,
                        avg_ms=round(statistics.mean(times), 3),
                        stddev_ms=round(statistics.stdev(times) if len(times) > 1 else 0, 3),
                        min_ms=round(min(times), 3), max_ms=round(max(times), 3),
                        rows_returned=10, scan_type="INSERT",
                        table_size_mb=size_mb, explain="N/A (procedural INSERT+DELETE)",
                    )
                    results.append(res)
                    print(f"    [{schema}] avg={res.avg_ms:.1f}ms  size={size_mb}MB")
                    continue

                if sql is None:
                    continue

                # Q4 needs a param
                params = None
                if "param_query" in qdef:
                    cur.execute(qdef["param_query"][schema])
                    row = cur.fetchone()
                    params = (row[0],) if row else ("__none__",)

                # Q5: run update + reset alternately
                if qname == "Q5_dataset_update":
                    reset_sql = qdef.get(f"{schema}_reset", "")
                    times = []
                    for _ in range(warmup):
                        cur.execute(sql)
                        conn.commit()
                        cur.execute(reset_sql)
                        conn.commit()
                    for _ in range(runs):
                        t0 = time.perf_counter()
                        cur.execute(sql)
                        conn.commit()
                        elapsed = (time.perf_counter() - t0) * 1000
                        times.append(elapsed)
                        cur.execute(reset_sql)
                        conn.commit()
                    row_count = cur.rowcount
                    explain_text = run_explain(cur, sql)
                    conn.rollback()
                else:
                    times, row_count = run_timed(cur, sql, params, warmup, runs)
                    explain_text = run_explain(cur, sql, params)

                size_mb = get_table_sizes(cur, schema)

                # Save explain output
                explain_path = os.path.join(explain_dir, f"{qname}_{schema}_{scale}.txt")
                with open(explain_path, "w") as f:
                    f.write(explain_text)

                res = BenchResult(
                    query=qname, schema=schema, scale=scale, description=desc,
                    avg_ms=round(statistics.mean(times), 3),
                    stddev_ms=round(statistics.stdev(times) if len(times) > 1 else 0, 3),
                    min_ms=round(min(times), 3), max_ms=round(max(times), 3),
                    rows_returned=row_count, scan_type=extract_scan_type(explain_text),
                    table_size_mb=size_mb, explain=explain_text,
                )
                results.append(res)
                print(f"    [{schema}] avg={res.avg_ms:.1f}ms  rows={row_count}  scan={res.scan_type}  size={size_mb}MB")

        cur.close()
        conn.close()

    # Write CSV (without full explain text — that's in files)
    fieldnames = [f for f in asdict(results[0]).keys() if f != "explain"]
    with open(output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in results:
            row = asdict(r)
            del row["explain"]
            writer.writerow(row)

    print(f"\nResults written to {output}")
    print(f"EXPLAIN plans written to {explain_dir}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run MAIQMag storage benchmark")
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--scales", nargs="+", default=["small", "medium", "large"],
                        choices=["small", "medium", "large"])
    parser.add_argument("--warmup", type=int, default=3)
    parser.add_argument("--runs", type=int, default=10)
    parser.add_argument("--output", default="results/benchmark_results.csv")
    parser.add_argument("--explain-dir", default="results/explain_plans")
    args = parser.parse_args()

    benchmark(args.dsn, args.scales, args.warmup, args.runs, args.output, args.explain_dir)
