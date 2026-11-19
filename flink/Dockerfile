# Flink с коннекторами для Kafka и Iceberg (REST-каталог + S3-хранилище Silo)
FROM flink:1.20-java17

ARG ICEBERG_VERSION=1.11.0
ARG KAFKA_CONNECTOR_VERSION=3.4.0-1.20
ARG MVN=https://repo1.maven.org/maven2

USER root

# 1. Коннектор Kafka (Flink читает из топика)
RUN curl -fSL -o /opt/flink/lib/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar \
    ${MVN}/org/apache/flink/flink-sql-connector-kafka/${KAFKA_CONNECTOR_VERSION}/flink-sql-connector-kafka-${KAFKA_CONNECTOR_VERSION}.jar

# 2. Iceberg для Flink 1.20 (версии Iceberg-JAR должны совпадать между собой)
RUN curl -fSL -o /opt/flink/lib/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar \
    ${MVN}/org/apache/iceberg/iceberg-flink-runtime-1.20/${ICEBERG_VERSION}/iceberg-flink-runtime-1.20-${ICEBERG_VERSION}.jar

# 3. S3FileIO (AWS SDK внутри одного JAR): Iceberg пишет файлы в Silo по S3-протоколу
RUN curl -fSL -o /opt/flink/lib/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
    ${MVN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# 4. Классы Hadoop, которые Iceberg-коннектор Flink ожидает на classpath
RUN curl -fSL -o /opt/flink/lib/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar \
    ${MVN}/org/apache/flink/flink-shaded-hadoop-2-uber/2.8.3-10.0/flink-shaded-hadoop-2-uber-2.8.3-10.0.jar

# 5. S3-плагин самого Flink (для чекпоинтов в бакет flink). Лежит в /opt/flink/opt, включаем как плагин
RUN mkdir -p /opt/flink/plugins/s3-fs-hadoop && \
    cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/

USER flink
