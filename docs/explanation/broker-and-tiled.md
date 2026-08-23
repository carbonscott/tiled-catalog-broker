# The broker and Tiled

The broker is not a fork of [Tiled](https://blueskyproject.io/tiled/), a wrapper around it,
or an alternative to it. It is a client that registers a particular *shape* into an
otherwise stock Tiled server, plus one array adapter that server loads.

Everything you interact with at read time is Tiled. The broker is not running.

## What Tiled already does

Tiled is a data access service: the server, the catalog database and its metadata search,
the HTTP API, the chunked array transport, the Python client, authentication, and storage
for arrays written into it. `http_register.py` adds nothing on top — it calls
`create_container`, `.new()`, and `write_array` with Tiled's own `DataSource`, `Asset`, and
`ArrayStructure` objects.

So no broker code executes when you run this:

```python
c = from_uri(URL, api_key=KEY)
c["BROAD_SIGMA"].search(Key("sigma") >= 0.04)
```

That is deliberate, and it is why reading a catalog needs only `pip install 'tiled[client]'`.

## What Tiled deliberately leaves open

Tiled has no opinion about what your data *means*. It will serve any tree you give it. What
it will not do is decide what counts as a record — the right call for a general service.

Point stock Tiled at a directory and it gives you one node per file. Sensible, and not what
a simulation sweep needs:

- 10,000 spectra stacked on axis 0 of one HDF5 dataset are **one** file. Tiled sees one
  array; you want to query one of ten thousand rows inside it.
- The parameters that make an entity findable — `sigma`, `Udd`, `Ja_meV` — are scalars
  *inside* the HDF5. To Tiled they are opaque bytes.
- Two producers describing the same physics with different field names both register fine,
  and neither is findable by a query written against the other.

None of these are Tiled defects. They are the questions Tiled leaves to whoever knows the
data, and the broker answers them for this kind of data.

## What the broker adds

- **A fixed shape.** Every dataset registers as the same three nested containers — Dataset
  → Entity → Artifact — so a query written against one dataset means something against the
  next (see [the data model](data-model.md)).
- **Entity extraction.** Three layouts say where the entity boundary falls (see
  [the three layouts](layouts.md)). Only one of them resembles the file-per-node default;
  the others turn *part* of a file into an independently queryable record.
- **Parameter harvesting.** The generator lifts HDF5 scalars, attributes, and named groups
  into container metadata, where Tiled's SQL-backed search can reach them. This is what
  makes `Key("sigma") >= 0.04` work at all.
- **A soft vocabulary.** Canonical ids for `material`, `method`, `producer` and friends, so
  cross-dataset facets stay honest when six groups onboard independently (see
  [the soft vocabulary](vocabulary.md)).
- **A scan/register split.** `tcb generate` records each artifact's shape and dtype into
  [a Parquet manifest](../reference/manifest.md); `tcb register` opens no HDF5 at all. That
  is what makes 10,000 entities cheap, and what lets a server that cannot see your
  filesystem be handed pointers to it anyway.
- **One server-side adapter.** The only broker code that runs inside the server. Tiled's
  stock HDF5 adapter materializes a whole dataset through dask before slicing, which is
  pathological for batched layouts; `LazyHDF5ArrayAdapter` slices in h5py instead (see
  [sliced reads](sliced-reads.md)). It is dispatched on the private mimetype
  `application/x-hdf5-broker`, which a server must be told about via
  `adapters_by_mimetype`.

## The dividing line

| | Tiled | The broker |
|---|---|---|
| Serving arrays over HTTP | ✓ | |
| Catalog database, metadata search | ✓ | |
| Client, auth, export formats | ✓ | |
| Storage for uploaded arrays | ✓ | |
| What counts as one record | | ✓ |
| Finding entities inside a file | | ✓ |
| Lifting HDF5 scalars into queryable metadata | | ✓ |
| Cross-dataset vocabulary | | ✓ |
| Registering without opening the files | | ✓ |
| Slicing batched arrays cheaply | | ✓ (adapter) |

In one sentence: **Tiled answers "serve me this array"; the broker answers "which array?"**

A useful consequence — nothing the broker registers is broker-specific. Delete the broker
and the catalog still works, because what is in the server is ordinary Tiled containers,
metadata, and data sources.
