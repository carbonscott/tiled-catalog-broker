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

Condition / verification gate: before deleting `bulk_register.py`, confirm `tcb register`
handles the bulk case (10k–100k entities) at acceptable speed post-#66. If unproven, that
benchmark is a step in the removal PR. The stale "when (not) to use" headers in both
`http_register.py` and `bulk_register.py` are reconciled in the same PR.
