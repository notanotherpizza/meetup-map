# Single image for all three components.
# Which role to run is controlled by the MEETUPMAP_ROLE env var:
#   seed    → runs seed/producer.py
#   worker  → runs worker/scraper.py    (not yet built)
#   sink    → runs sink/consumer.py     (not yet built)
#
# Build:  docker build -t meetupmap .
# Run seed locally:
#   docker run --env-file .env -v $(pwd)/certs:/app/certs meetupmap seed

FROM python:3.12-slim

WORKDIR /app

# Install system deps for Playwright's Chromium
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates \
    # Chromium runtime deps
    libnss3 libatk1.0-0 libatk-bridge2.0-0 libcups2 libdrm2 \
    libxkbcommon0 libxcomposite1 libxdamage1 libxfixes3 \
    libxrandr2 libgbm1 libasound2 \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]" \
    && playwright install chromium --with-deps

COPY . .

# Certs mounted at runtime: docker run -v $(pwd)/certs:/app/certs ...
ENV KAFKA_SSL_CA_FILE=/app/certs/ca.pem
ENV KAFKA_SSL_CERT_FILE=/app/certs/service.cert
ENV KAFKA_SSL_KEY_FILE=/app/certs/service.key

ENTRYPOINT ["python", "-m"]
CMD ["seed.producer"]

# Override CMD for other roles:
#   docker run meetupmap worker.scraper
#   docker run meetupmap sink.consumer
