FROM --platform=linux/amd64 maven:3.9.9-eclipse-temurin-17 AS dependencies

WORKDIR /dependencies
COPY pom.xml .
RUN mvn --batch-mode --quiet dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/runtime-jars

FROM ghcr.io/astral-sh/uv:0.12.5 AS uv

FROM --platform=linux/amd64 flink:1.20.5-scala_2.12-java17

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 python3-dev ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_SYSTEM_PYTHON=1 \
    UV_PYTHON_DOWNLOADS=0 \
    UV_PYTHON=/usr/bin/python3
RUN uv pip install --system --no-cache apache-flink==1.20.5 && \
    ln -sf /usr/bin/python3 /usr/local/bin/python

COPY --from=dependencies /runtime-jars/ /opt/flink/lib/

# The official 1.20.5 image already ships the Prometheus reporter as the
# /opt/flink/plugins/metrics-prometheus plugin; FLINK_PROPERTIES enables it.

COPY logic.py job.py /opt/flink/usrlib/
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/usrlib /opt/flink/checkpoints /opt/flink/savepoints

ENV PYTHONPATH=/opt/flink/usrlib
ENV PYFLINK_CLIENT_EXECUTABLE=/usr/bin/python3
USER flink
