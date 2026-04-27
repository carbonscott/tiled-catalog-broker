# AmSC Automatic Catalog Ingestion

```sh
cd amsc-connector
make tiled-up
make connector-up
uv run --project .. --env-file .env ./scripts/register_test_data.py
```
