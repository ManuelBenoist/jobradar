# JobRadar — Agent Guidelines

## Project Layout

| Directory | Purpose |
|-----------|---------|
| `src/lambda/{adzuna,france_travail,jooble,jsearch}/` | Ingestion Lambda functions (Python 3.11, zip-deployed) |
| `src/spark/transform.py` | PySpark Silver-layer transform (NLP embeddings, dedup) |
| `src/scripts/generate_profile.py` | Generates `transform/seeds/ideal_profile_vector.csv` |
| `transform/` | dbt project (dbt-athena-community adapter, Athena Presto SQL) |
| `api/main.py` | FastAPI + Mangum (container Lambda for querying Athena) |
| `ui/app.py` | Streamlit dashboard |
| `terraform/` | Terraform IaC for all AWS resources |

## Testing (greenfield)

**No tests exist yet** — everything must be created from scratch. Layout to follow:
- Python tests: `tests/` mirroring `src/` structure (e.g., `tests/lambda/`, `tests/spark/`, `tests/api/`)
- dbt tests: `transform/tests/` (generic + custom), add `.yml` schema tests in model dirs
- Coverage target: 90% minimum
- Use `pytest` (already in `requirements.txt`), `pytest-cov` likely needed

## Key Commands

| Command | Description |
|---------|-------------|
| `make setup` | Create venv + install all deps from `requirements.txt` |
| `pip install ruff pytest` | CI lint/test dependencies |
| `ruff check .` | Lint (config in `pyproject.toml`: line-length=88, select=E,F,B,I) |
| `pytest` | Run all tests (currently exits 0 with "no tests found") |
| `make dbt-setup` | `dbt deps && dbt seed` |
| `make dbt-run` | `dbt run` (from `transform/`) |
| `cd transform && dbt test` | Run dbt data tests |
| `python src/spark/transform.py` | Run PySpark transform locally (needs Java 11, AWS creds) |
| `python src/scripts/generate_profile.py` | Regenerate ideal profile seed |
| `source .venv/bin/activate` | Activate venv |
| `python -m pytest --cov=src --cov-report=term-missing` | Run tests with coverage report (target: 90% on src/) |
| `make clean` | Remove `__pycache__`, `.pytest_cache`, `.ruff_cache`, dbt artifacts |

## CI/CD Pipeline (GitHub Actions)

- `ci.yml`: Runs on push to `main`/`develop`, PR to `main` — runs `ruff check .` then `pytest` (non-blocking if no tests)
- `data_pipeline.yml`: Daily at 05:15 UTC — Spark Silver → dbt Gold (sequential jobs)
- `api_cd.yml`: On `api/` changes to `main` — Docker build/push to ECR, update Lambda
- `deploy_infra.yml`: On `src/lambda/` or `terraform/` changes to `main` — runs Terraform apply

## Architecture Notes

- **Medallion**: Bronze (S3 JSON raw) → Silver (PySpark → S3 Parquet) → Gold (dbt → S3 Parquet via Athena)
- **AWS region**: `eu-west-3` (Paris) everywhere
- **S3 buckets**: `jobradar-raw-manuel-cloud`, `jobradar-processed-manuel-cloud`, `jobradar-curated-manuel-cloud`, `jobradar-athena-results-manuel-cloud`
- **dbt**: Uses `dbt-athena-community` adapter, not standard dbt-postgres. Profiles use `env_var()` for S3 paths (`DBT_S3_STAGING_DIR`, `DBT_S3_DATA_DIR`). Materializations: staging=view, marts=table (hive/parquet)
- **Lambda naming**: Ingestion: `jobradar-ingest-{source}`, handler: `ingest_{source}.lambda_handler`; API: `jobradar-api-serverless-v2` (container)
- **NLP**: `all-MiniLM-L6-v2` via `sentence-transformers`. Embedding dimension = 384. Cosine similarity computed in SQL (Athena/Presto `UNNEST(SEQUENCE(1, 384))`)
- **dbt vars**: Scoring parameters in `dbt_project.yml` (`base_score`, `bonus_junior_flag`, `penalty_senior`, etc.)
- **API auth**: `X-API-Key` header required; CORS allows Streamlit Cloud and localhost:8501
- **Secrets**: `.env` for local dev, GitHub Secrets for CI/CD, AWS env vars for Lambda
- **`.gitignore`**: Lambda dirs are mostly ignored except `*.py` files — pip-installed libs in lambda dirs are not committed

## dbt Testing Context

Existing schema tests in `transform/models/staging/`:
- `sources.yml`: `unique` + `not_null` on `job_id`, `accepted_values` on `source_name` (`'Adzuna', 'France Travail', 'Jooble', 'JSearch'`), `not_null` on `published_at`
- `stg_silver_jobs.yml`: `unique` + `not_null` on `job_id`, `not_null` on `ingestion_date`, `accepted_values` on `is_red_flag` (`true`, `false`)

Custom test SQL files go in `transform/tests/`. No custom tests exist yet.
