FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
COPY shared/ shared/
COPY worker/ worker/
COPY map/ map/
COPY community/ community/
COPY batch_worker/ batch_worker/
RUN pip install ".[download]"
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENV APP_MODE=batch
ENTRYPOINT ["./entrypoint.sh"]
