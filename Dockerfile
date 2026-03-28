FROM python:3.12-slim
WORKDIR /app
COPY pyproject.toml .
COPY shared/ shared/
COPY sink/ sink/
COPY worker/ worker/
COPY seed/ seed/
COPY map/ map/
RUN pip install -e .
