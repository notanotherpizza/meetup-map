FROM python:3.12-slim
WORKDIR /app

RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY shared/ shared/
COPY download_service/ download_service/
RUN pip install ".[download]"
# Pre-install the DuckDB postgres extension at build time so cold starts don't
# pay a network fetch on first request.
RUN python -c "import duckdb; duckdb.connect().execute('INSTALL postgres')"

EXPOSE 8000
CMD ["uvicorn", "download_service.main:app", "--host", "0.0.0.0", "--port", "8000"]
