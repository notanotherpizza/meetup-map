FROM python:3.12-slim
WORKDIR /app
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
COPY pyproject.toml .
COPY shared/ shared/
COPY sink/ sink/
COPY worker/ worker/
COPY seed/ seed/
COPY map/ map/
COPY discovery/ discovery/
COPY community/ community/
RUN pip install .
COPY entrypoint.sh .
RUN chmod +x entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]