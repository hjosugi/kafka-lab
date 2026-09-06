FROM docker.io/library/flink:2.2.1
ADD --chmod=0644 https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.2/flink-sql-connector-kafka-5.0.0-2.2.jar /opt/flink/lib/flink-sql-connector-kafka.jar
RUN echo '5605c691d11a501382c383fecba37a7a552467da5ab7ba904ef5d6f3d62c5616  /opt/flink/lib/flink-sql-connector-kafka.jar' | sha256sum --check -
USER root
RUN mkdir -p /checkpoints && chown flink:flink /checkpoints
USER flink
