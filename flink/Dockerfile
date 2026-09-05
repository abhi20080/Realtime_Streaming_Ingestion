FROM --platform=linux/amd64 maven:3.9.16-eclipse-temurin-17 AS dependencies

WORKDIR /dependencies
COPY pom.xml .
RUN mvn --batch-mode --quiet dependency:copy-dependencies \
    -DincludeScope=runtime \
    -DoutputDirectory=/runtime-jars

FROM ghcr.io/astral-sh/uv:0.12.10 AS uv

FROM --platform=linux/amd64 flink:2.2.1-scala_2.12-java17

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        ca-certificates curl libexpat1 && \
    rm -rf /var/lib/apt/lists/*

COPY --from=uv /uv /usr/local/bin/uv
ENV UV_PYTHON_INSTALL_DIR=/opt/python \
    VIRTUAL_ENV=/opt/flink/venv
RUN uv python install 3.11.16 && \
    uv venv --python 3.11.16 /opt/flink/venv && \
    uv pip install --python /opt/flink/venv/bin/python --no-cache apache-flink==2.2.1
ENV PATH="/opt/flink/venv/bin:${PATH}"

COPY --from=dependencies /runtime-jars/ /opt/flink/lib/

# The official 2.2.1 image already ships the Prometheus reporter as the
# /opt/flink/plugins/metrics-prometheus plugin; FLINK_PROPERTIES enables it.

COPY logic.py row_mapping.py jdbc_compat.py job.py check_runtime.py /opt/flink/usrlib/
RUN mkdir -p /opt/flink/checkpoints /opt/flink/savepoints && \
    chown -R flink:flink /opt/flink/usrlib /opt/flink/checkpoints /opt/flink/savepoints

ENV PYTHONPATH=/opt/flink/usrlib
ENV PYFLINK_CLIENT_EXECUTABLE=/opt/flink/venv/bin/python
USER flink
RUN CLICKHOUSE_PASSWORD=build-check python /opt/flink/usrlib/check_runtime.py
