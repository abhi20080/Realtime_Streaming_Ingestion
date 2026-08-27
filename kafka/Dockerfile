FROM apache/kafka:3.9.2

ARG JMX_EXPORTER_VERSION=1.6.0

USER root
RUN mkdir -p /opt/jmx-exporter
ADD https://github.com/prometheus/jmx_exporter/releases/download/${JMX_EXPORTER_VERSION}/jmx_prometheus_javaagent-${JMX_EXPORTER_VERSION}.jar /opt/jmx-exporter/jmx_prometheus_javaagent-1.6.0.jar
COPY kafka.yml /opt/jmx-exporter/kafka.yml
RUN chmod 0444 \
      /opt/jmx-exporter/jmx_prometheus_javaagent-1.6.0.jar \
      /opt/jmx-exporter/kafka.yml

USER appuser
