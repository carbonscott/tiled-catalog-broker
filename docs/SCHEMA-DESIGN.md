# Schema Design: Multi-Modal Data Broker

**Date:** 2026-02-20
**Status:** Proposal
**Related:** Issue #38 (Generic data broker and contract plan)

---

## Problem

The current catalog stores all entities from all modalities in a single
flat namespace.  All 27,627 entities (VDP, EDRIXS, NiPS3, RIXS,
Challenge, SEQUOIA) sit as direct children of the root node.  There is
no modality column, no dataset grouping, and no way to scope a query
to a single dataset.

When a user queries `Key("tenDq") > 3.0`, the database extracts
`$.tenDq` from every JSON metadata blob — including 10K VDP entities
and 7.6K NiPS3 entities that don't have that key at all.  At 27K
entities this is fast enough, but the design doesn't scale.

---

## Inspiration: ArrayLake Data Model

ArrayLake (Earthmover) organizes scientific array data as:

```
Organization → Repo → Group → Array
```

Key ideas we adopt:

- **Repo as the unit of organization** — each dataset is a self-contained
  repository with its own namespace and metadata.
- **Organization is metadata, not structure** — it describes who owns the
  data, but doesn't add a structural layer.
- **Groups carry metadata** — physics parameters live on the entity
  container, not in a separate table.
- **Arrays are chunked leaves** — artifacts are the terminal data,
  served via chunked access.

What differs for us:

- ArrayLake optimizes for **few repos with large arrays** (satellite
  imagery, climate grids).  We have **many entities with small arrays**
  (27K+ Hamiltonians, each with ~10 modest arrays).
- We need **cross-entity metadata queries** (find all Hamiltonians
  where Ja > 0.5).  ArrayLake's Zarr metadata isn't queryable at scale.
- Our data is immutable once generated; versioning is not essential.

---

## Proposed Hierarchy

Three levels: **Dataset → Entity → Artifact**.

```
/ (root)
│
├── VDP/                              ← dataset container
│   metadata: {
│     organization: "MAIQMag",
│     data_type: "simulation",
│     producer: "Sunny.jl",
│     producer_version: null,         ← slot defined, populate later
│     material: "generic spin model",
│     created_at: "2025-12-07",
│     n_entities: 10000,
│   }
│   │
│   ├── H_636ce3e4/                   ← entity container
│   │   metadata: {Ja_meV: 0.509, Jb_meV: 0.745, ...}
│   │   ├── mh_powder_30T            ← array artifact
│   │   ├── ins_12meV                ← array artifact
│   │   └── ...
│   └── H_abc12345/
│       └── ...
│
├── EDRIXS/                           ← dataset container
│   metadata: {
│     organization: "MAIQMag",
│     data_type: "simulation",
│     producer: "EDRIXS",
│     producer_version: null,
│     material: "NiPS3",
│   }
│   │
│   ├── H_edx00000/
│   │   metadata: {tenDq: 3.45, F2_dd: 12.3, ...}
│   │   └── rixs
│   └── ...
│
├── RIXS/                             ← dataset container
│   metadata: {
│     organization: "MAIQMag",
│     data_type: "experimental",      ← not simulation
│     material: "NiPS3",
│     producer: null,                 ← N/A
│     facility: "LCLS",
│     instrument: "qRIXS",
│   }
│   └── ...
│
└── SEQUOIA/
    metadata: {
      organization: "MAIQMag",
      data_type: "experimental",
      material: "NiPS3",
      facility: "SNS",
      instrument: "SEQUOIA",
    }
    └── ...
```

### Why dataset containers at the top

| Concern | Current (flat) | With dataset containers |
|---------|---------------|------------------------|
| "Give me only EDRIXS" | Impossible | `client["EDRIXS"]` |
| Query within a modality | Scans all 27K | `client["EDRIXS"].search(...)` scans 10K |
| Cross-modal query | Same | `client.search(...)` at root still works |
| Add new modality | Insert flat | Create container, insert under it |
| Tiled compatibility | Already works | Already works — nested containers are native |

### Why organization is metadata, not structure

Organization describes **who owns the data** — an access-control and
provenance concern.  Making it a structural layer (Organization → Dataset →
Entity) would add depth without benefit: all our data currently belongs
to one organization (MAIQMag), and if a second organization contributes
data, we can filter by `Key("organization") == "X"` at the root level.

For simulations, the `producer` and `producer_version` fields capture
**how the data was generated** — the simulation code, its version, and
optionally the commit hash.  These slots should be defined now even if
not yet populated, so the schema expectation is documented.

### Why material is metadata, not structure

Some team members have suggested placing materials at the top level
(`root → material → dataset → entity → artifact`), since nearly all
current datasets describe NiPS3.  We keep material as a metadata field
on the dataset container for the same reasons as organization:

1. **Lopsided tree.**  Most current datasets are NiPS3-related (EDRIXS,
   NiPS3_Multimodal, RIXS, SEQUOIA), while VDP uses a generic spin
   model.  A structural `NiPS3/` node with most children and a lone
   `generic/` node is not a useful partition.

2. **Ambiguous classification.**  VDP is a generic spin model — it is
   not tied to any specific material.  Forcing it into a material bucket
   introduces an artificial choice that metadata avoids.

3. **Cross-cutting queries are easy.**  At the dataset container level
   there are only ~6 rows, so `client.search(Key("material") == "NiPS3")`
   is instant.  The same pattern works for `facility`, `producer`, or
   any other descriptor — no structural change required.

4. **Extra depth costs more than it saves.**  A 4th structural level
   lengthens every access path
   (`client["NiPS3"]["EDRIXS"]["H_edx00000"]`) and adds complexity to
   the closure table, with no query-performance benefit at our scale.

The general principle: **promote a descriptor to structure only when it
partitions the data into roughly equal, stable groups that users
routinely navigate by**.  Dataset type meets this criterion (simulation
vs. experimental, different parameter spaces).  Material, organization,
and facility do not — they are better served as queryable metadata.

---

## Dataset Container Metadata: Recommended Fields

### Common fields (all datasets)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `organization` | string | Yes | Owning project or group |
| `data_type` | string | Yes | `"simulation"` or `"experimental"` |
| `material` | string | No | Target material or system |
| `description` | string | No | Human-readable summary |
| `created_at` | string | No | When the dataset was generated |

### Simulation-specific fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `producer` | string | Yes | Code that generated the data |
| `producer_version` | string | No | Version tag or semver |
| `producer_commit` | string | No | Git commit hash |
| `prior_distribution` | string | No | How parameter ranges were sampled |

### Experimental-specific fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `facility` | string | Yes | Where the data was collected |
| `instrument` | string | No | Instrument or beamline |
| `proposal_id` | string | No | Experiment proposal number |

Additional fields can be added freely — these are recommendations, not
a rigid schema.

---

## How This Maps to Tiled's SQLite Schema

No schema changes are needed.  The dataset container is just another
row in the `nodes` table with `parent = root_id` and
`structure_family = 'container'`.

```
nodes table:

id=0   parent=NULL  key=""          family=container  ← root
id=1   parent=0     key="VDP"       family=container  ← dataset
id=2   parent=0     key="EDRIXS"    family=container  ← dataset
id=100 parent=1     key="H_636ce3e4" family=container  ← entity (under VDP)
id=101 parent=100   key="mh_powder_30T" family=array   ← artifact
```

Tiled's `client["EDRIXS"].search(Key("tenDq") > 3.0)` naturally scopes
the SQL query to `WHERE parent = 2`, touching only EDRIXS entities.

---

## Dataset YAML Config: The Contract

The `key` field is an **explicit contract** between the data provider
and the broker.  It defines the container name in the catalog and must
be agreed upon at onboarding time.

```yaml
# Dataset config
key: EDRIXS                       # ← container key (contract with provider)
label: EDRIXS Simulations         # human-readable display name
base_dir: /path/to/EDRIXS
metadata:                          # ← optional provenance block
  organization: MAIQMag
  data_type: simulation
  producer: EDRIXS
  producer_version: null
  material: NiPS3
```

`key` is required and immutable after first ingestion — it becomes the
Tiled container key that users address as `client["EDRIXS"]`.  `label`
remains the human-readable display name.  `metadata` is optional; if
absent, the container is created with `{label: "..."}` only.

---

## Implementation Complexity

### Broker code (`tiled_poc/broker/`)

#### `bulk_register.py` — HIGH impact

The core SQL registration.  Three specific changes:

**1. Entity parent ID (line 305)**

Currently hardcoded:
```python
INSERT INTO nodes (parent, key, ...) VALUES (0, :key, ...)
```

Must become parameterized.  Before entity inserts, create the dataset
container and capture its `id`:

```python
# New: insert dataset container (once per broker-ingest call)
conn.execute(text("""
    INSERT INTO nodes (parent, key, structure_family, metadata, specs, access_blob)
    VALUES (0, :key, 'container', :metadata, '[]', '{}')
"""), {"key": dataset_key, "metadata": json.dumps(dataset_metadata)})
dataset_node_id = conn.execute(text("SELECT last_insert_rowid()")).scalar()

# Then: entity INSERT uses dataset_node_id instead of 0
INSERT INTO nodes (parent, ...) VALUES (:dataset_parent, ...)
```

**2. Closure table rebuild (lines 414-439)**

Currently handles 2 levels (root→entity→artifact).  Add a depth=3
join for root→dataset→entity→artifact:

```sql
-- Existing depth=2: root → entity → artifact
INSERT INTO nodes_closure (ancestor, descendant, depth)
SELECT gp.parent, n.id, 2
FROM nodes n JOIN nodes gp ON n.parent = gp.id
WHERE gp.parent IS NOT NULL;

-- NEW depth=3: root → dataset → entity → artifact
INSERT INTO nodes_closure (ancestor, descendant, depth)
SELECT ggp.parent, n.id, 3
FROM nodes n
JOIN nodes gp ON n.parent = gp.id
JOIN nodes ggp ON gp.parent = ggp.id
WHERE ggp.parent IS NOT NULL;
```

**3. Verification query (line 484)**

Currently `WHERE parent = 0`.  Must detect dataset containers first,
then drill into one to verify entities.

#### `http_register.py` — MEDIUM impact

Line ~155: `client.create_container(key=ent_key, ...)` at root.

```python
# New: check/create dataset container, nest entities under it
if dataset_key not in client:
    dataset_container = client.create_container(
        key=dataset_key, metadata=dataset_metadata
    )
else:
    dataset_container = client[dataset_key]

# Entities under dataset container (not root)
ent_container = dataset_container.create_container(key=ent_key, metadata=metadata)
```

#### `catalog.py` — LOW impact

Pass `dataset_key` and `dataset_metadata` through to `prepare_node_data()`
and `bulk_register()`.  Signature change only.

#### `cli.py` — LOW impact

Read `key` and `metadata` from the YAML config, pass through:

```python
dataset_key = config.get("key", config["label"])   # fallback to label
dataset_metadata = config.get("metadata", {"label": config["label"]})
register_dataset(engine, ent_df, art_df, base_dir, label,
                 dataset_key=dataset_key, dataset_metadata=dataset_metadata)
```

#### Backward compatibility

All new parameters default to `None`.  When `dataset_key is None`, the
broker inserts entities at `parent=0` (current behavior).  This allows
incremental adoption — new datasets get containers, old ones keep
working.

### Example repo (`cwang31-data-broker/`)

#### `demo_multimodal.py` — ~11 lines, all mechanical

Every `client[entity_key]` becomes `client[dataset_key][entity_key]`,
and every root-level `client.search(...)` becomes
`client[dataset_key].search(...)`.

**Mode B (per-dataset retrieval):**
```python
# Before
_h = client[_key]

# After — _s already has "label"; use "key" from YAML instead
_h = client[_dataset_key][_key]
```

**Mode A (query-based):**
```python
# Before
_subset = _apply_filters(client, _s["filters"])

# After — scope to dataset
_dataset_client = client[_s["key"]]
_subset = _apply_filters(_dataset_client, _s["filters"])
```

**Deep-dive sections (6 hardcoded accesses):**
```python
# Before                              After
client["H_636ce3e4"]                  client["VDP"]["H_636ce3e4"]
client["H_edx00000"]                  client["EDRIXS"]["H_edx00000"]
client["H_mm_1"]                      client["NiPS3_Multimodal"]["H_mm_1"]
client["H_rixs_052"]                  client["RIXS"]["H_rixs_052"]
client["H_challang"]                  client["Challenge"]["H_challang"]
client["H_seq_Ei28"]                  client["SEQUOIA"]["H_seq_Ei28"]
```

#### `datasets/*.yaml` — add `key` field

```yaml
# vdp.yaml
key: VDP
label: VDP
base_dir: /sdf/.../vdp/data/schema_v1

# edrixs.yaml
key: EDRIXS
label: EDRIXS
base_dir: /sdf/.../EDRIXS
```

#### Reference notebooks (`tiled_poc/examples/`) — same mechanical pattern

`client[key]` → `client["VDP"][key]` and
`client.search(...)` → `client["VDP"].search(...)`.

### Summary

| Component | Impact | Lines | Backward compat |
|-----------|--------|-------|-----------------|
| `bulk_register.py` | High | ~30 | Optional param |
| `http_register.py` | Medium | ~15 | Optional param |
| `catalog.py` | Low | ~5 | Signature only |
| `cli.py` | Low | ~5 | Config-driven |
| `demo_multimodal.py` | Medium | ~11 | Mechanical nesting |
| `datasets/*.yaml` | Low | +1 field each | New field |

---

## Future Considerations

### PostgreSQL with JSONB indexes

If the catalog grows beyond 100K entities, PostgreSQL's `JSONB` type
supports GIN indexes on JSON paths — enabling indexed lookups without
full-table scans.  Tiled already supports PostgreSQL as a backend.

### Cross-modal discovery

With dataset containers, users can discover modalities before querying:

```python
for key in client.keys():
    ds = client[key]
    print(f"{key}: {ds.metadata.get('data_type')}, {len(ds)} entities")

edrixs = client["EDRIXS"]
subset = edrixs.search(Key("tenDq") > 3.0)
```

Because descriptors like `material` and `facility` are metadata fields
on the dataset containers, cross-cutting queries are straightforward:

```python
from tiled.queries import Key

# "Give me all NiPS3 datasets for my ML task"
nips3 = client.search(Key("material") == "NiPS3")

# "Show me everything from SNS"
sns = client.search(Key("facility") == "SNS")

# Combine criteria
nips3_exp = client.search(Key("material") == "NiPS3").search(
    Key("data_type") == "experimental"
)
```

These queries scan only the ~6 dataset-container rows (not the 27K+
entity rows), so they are effectively free.

### Relationship to ticket #38

The dataset landscape in ticket #38 identifies 4 structural patterns
(per-entity files, batched arrays, per-entity groups, experimental).
The hierarchy proposed here is orthogonal to storage layout — each
dataset container holds entities regardless of physical storage.  The
manifest generator translates from physical layout to the standard
(uid, key, type, file, dataset, index) schema.
