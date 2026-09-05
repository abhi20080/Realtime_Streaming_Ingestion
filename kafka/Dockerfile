FROM apache/kafka:3.9.2

USER root
RUN mkdir -p /opt/jmx-exporter
# make kafka-agent downloads this on the host, outside BuildKit's network.
COPY jmx_prometheus_javaagent-1.6.0.jar /opt/jmx-exporter/jmx_prometheus_javaagent-1.6.0.jar
COPY kafka.yml /opt/jmx-exporter/kafka.yml
RUN chmod 0444 \
      /opt/jmx-exporter/jmx_prometheus_javaagent-1.6.0.jar \
      /opt/jmx-exporter/kafka.yml

USER appuser
