# Single registration route (HTTP), bulk SQL path removed

Onboarding converges on one registration route: `tcb register` (the HTTP path via
`http_register.py`). The bulk SQL path (`tcb ingest` / `bulk_register.py`, ~546 lines) is
removed, and `tests/test_generic_registration.py` is migrated off `prepare_node_data`.

`bulk_register` only ever existed as a performance workaround — its own docstring says it
"will be removed once HTTP registration performance is sufficient for all use cases." The
#66 parallelization (per-entity `ThreadPoolExecutor`, targeting the socket-recv bottleneck)
was the work to close that gap. Two registration routes for the same job is exactly the
complexity this simplification targets, and the contract (the Parquet manifest) is
identical for both, so nothing in the data model is lost.

`tests/test_generic_registration.py` covers the route without a server: it drives
`_register_one_entity` against a mock parent container and asserts on what registration
sends — entity key, metadata, Mode-A locators, and each artifact's DataSource.

Registration performance is characterized on an ongoing basis in the `tiled-bench` repo
(`BENCHMARK-PLAN.md`), which is where the bulk-scale numbers live.
