FROM python:3.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# NOTE: pyproject.toml/shared/download_service are duplicated into this
# directory (not symlinked) because the Aiven app-deploy tool scopes the
# Docker build context to build_path itself, with no way to point at a
# Dockerfile with a custom name while keeping a wider context (the
# underlying Aiven platform supports that via a separate containerfile_path
# field, but this specific deploy tool doesn't expose it). Keep these in
# sync with the canonical shared/ and download_service/ at the repo root,
# or migrate this app to a Console-created deployment and delete this
# directory entirely.
COPY pyproject.toml .
COPY shared/ shared/
COPY download_service/ download_service/
RUN pip install ".[download]"
# Pre-install the DuckDB postgres extension at build time so cold starts don't
# pay a network fetch on first request.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL postgres')"

EXPOSE 8000
CMD ["uvicorn", "download_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
