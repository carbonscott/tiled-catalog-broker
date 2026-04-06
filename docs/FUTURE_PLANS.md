# Future Plans

## Material Registry with External Database Lookup

### Problem
Materials are currently stored as flat string IDs (`NiPS3`, `NiO`) in
`catalog_model.yml`. This means users can't query by crystal system,
space group, lattice parameters, or link to external databases.

### Design
A **material registry** (`schema/material_registry.yml`) acts as a
local cache of structured material metadata, populated once per material
via the Materials Project and CIF database APIs.

#### Registry format

```yaml
# schema/material_registry.yml
NiPS3:
  label: "NiPS3"
  description: Nickel phosphorus trisulfide (van der Waals antiferromagnet)
  chemical_formula: "NiPS3"
  mp_id: "mp-676040"
  crystal_system: monoclinic
  space_group: "C2/m"
  space_group_number: 12
  lattice:
    a: 5.812
    b: 10.070
    c: 6.632
    alpha: 90.0
    beta: 107.16
    gamma: 90.0
  material_class: [van_der_Waals, antiferromagnet, insulator]

NiO:
  label: "NiO"
  description: Nickel oxide (antiferromagnetic insulator)
  chemical_formula: "NiO"
  mp_id: "mp-19009"
  crystal_system: cubic
  space_group: "Fm-3m"
  space_group_number: 225
  lattice:
    a: 4.177
    b: 4.177
    c: 4.177
    alpha: 90.0
    beta: 90.0
    gamma: 90.0
  material_class: [antiferromagnet, insulator]
```

#### Onboarding flow

```
1. New material arrives
   $ broker-material-lookup NiPS3
   → Queries Materials Project API (requires free API key)
   → Writes structured entry to schema/material_registry.yml
   → "Added NiPS3 (mp-676040, C2/m, monoclinic)"

2. User references it in dataset YAML
   material: NiPS3

3. At registration time (broker-ingest or broker-register)
   → Looks up "NiPS3" in material_registry.yml
   → Merges all structured fields into dataset container metadata
   → crystal_system, space_group, mp_id, lattice, etc. become queryable

4. Users can query
   client.search(Key('crystal_system') == 'monoclinic')
   client.search(Key('mp_id') == 'mp-676040')
   client.search(Key('space_group') == 'C2/m')
```

#### Implementation pieces

1. **`schema/material_registry.yml`** — the registry file, starts with
   NiPS3 and NiO pre-populated
2. **`broker/material_lookup.py`** — CLI tool (~50 lines):
   - Takes a formula or material name
   - Queries Materials Project via `mp-api` (`pip install mp-api`)
   - Extracts: mp_id, crystal_system, space_group, lattice parameters
   - Writes/updates `material_registry.yml`
   - Optionally queries COD/ICSD for experimental CIF data
3. **Change to `cli.py`** — at registration, look up the material in the
   registry and merge into `dataset_metadata` dict before calling
   `register_dataset()` or `register_dataset_http()`
4. **Change to `inspect.py`** — show registered materials with their
   structured fields in the draft YAML TODO comments

#### Dependencies

- `mp-api` (Materials Project Python client) — free API key required
  from https://materialsproject.org
- `pymatgen` — for structure analysis (SpacegroupAnalyzer) if computing
  from CIF files directly
- Neither is needed at registration time — only for the one-time lookup

#### Since metadata is now updateable

The `broker-register` and `broker-ingest` commands now support metadata
updates via config hash change detection. This means material registry
fields can be added after initial registration:

1. Populate the material registry
2. Re-run `broker-ingest` or `broker-register` with the same YAML configs
3. The config hash will differ → metadata is merged/updated
4. No need to re-generate manifests or re-register entities

### Standards Mapping

The registry fields are drawn from established standards:

| Registry field | Standard | Notes |
|---|---|---|
| `chemical_formula` | CIF `_chemical_formula_sum` | Hill ordering convention |
| `mp_id` | Materials Project | Persistent, DOI-backed identifier |
| `crystal_system` | CIF / NXsample `unit_cell_class` | Enumerated: triclinic...cubic |
| `space_group` | CIF `_space_group_name_H-M` | Hermann-Mauguin symbol |
| `space_group_number` | CIF `_space_group_IT_number` | International Tables number |
| `lattice` | CIF `_cell_length_*` / NXsample `unit_cell_abc` | In angstroms and degrees |
| `material_class` | Custom vocabulary | No standard covers magnetic classification well |

### Experimental Condition Metadata (NXsample-inspired)

For experimental datasets (RIXS, SEQUOIA), per-entity metadata should
adopt NXsample field naming conventions:

```yaml
# Entity-level metadata (in manifest or HDF5)
temperature_K: 4.0
magnetic_field_T: 0.0
incident_energy_meV: 28.0
sample_situation: single_crystal   # NXsample enum: single_crystal, powder, etc.
sample_orientation: [0, 0, 1]      # Crystal orientation (hkl of scattering plane)
```

These are already supported by the manifest generator (any scalar or
small array in the HDF5 becomes a metadata column). The naming convention
just needs to be documented so producers use consistent field names.

### PROV-O Vocabulary Alignment

The existing provenance fields already map to PROV-O concepts:

| Current field | PROV-O concept | Relationship |
|---|---|---|
| `producer` | `prov:SoftwareAgent` | `wasAssociatedWith` |
| `code_version` | Agent attribute | Version of the software agent |
| `created_at` | `prov:generatedAtTime` | When the entity was generated |
| `material` | `prov:Entity` (input) | `used` / `wasDerivedFrom` |
| `round` | `prov:Activity` attribute | Iteration of the generation process |

No action needed — just documenting the alignment for future
interoperability if the project ever needs to export to RDF/linked data.

### Not Planned

- **Full PROV-O/RDF implementation** — overkill for current scale
- **OPTIMADE server** — useful as a query client for lookup, not as
  something we serve
- **Storing full CIF files** in the catalog — wrong granularity; the
  registry stores the key fields
- **Runtime API calls** at registration — the registry is a local cache;
  no network dependency during ingest
