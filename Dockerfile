FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
COPY shared/ shared/
COPY sink/ sink/
COPY worker/ worker/
COPY seed/ seed/
COPY map/ map/
RUN pip install .
RUN playwright install chromium --with-deps