# syntax=docker/dockerfile:1.7
FROM python:3.11.16-slim-bookworm

COPY --from=ghcr.io/astral-sh/uv:0.12.10 /uv /uvx /bin/

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
RUN useradd --create-home --uid 10001 producer

COPY pyproject.toml ./
RUN uv pip install --system --no-cache "confluent-kafka==2.15.0"

COPY producer.py workload.py delivery.py logging_utils.py ./
USER producer

ENTRYPOINT ["python", "-u", "producer.py"]
