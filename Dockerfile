ARG JAVA_VERSION=11

FROM maven:3.8.8-eclipse-temurin-${JAVA_VERSION} AS benchmarks

WORKDIR /app

COPY /benchmarks .

RUN mvn clean package compile package

FROM maven:3.8.8-eclipse-temurin-${JAVA_VERSION} AS build
ARG SKIP_TESTS=true
ARG HTTP_CLIENT=okhttp

WORKDIR /app

COPY /flink .

ARG FROCKSDBJNI_JAR=frocksdb/java/target/rocksdbjni-6.20.3-linux64.jar
COPY ${FROCKSDBJNI_JAR} /tmp/rocksdbjni.jar

ARG SKIP_FROCKSDBJNI=false

RUN --mount=type=cache,target=/root/.m2 \
    if [ "$SKIP_FROCKSDBJNI" = "true" ]; then \
      mvn clean install -T 8 -DskipTests -Dspotless.check.skip=true -Drat.skip=true -Dcheckstyle.skip; \
    else \
      mkdir -p /root/.m2/repository/com/ververica/frocksdbjni/6.20.3-ververica-2.0 && \
      cp /tmp/rocksdbjni.jar /root/.m2/repository/com/ververica/frocksdbjni/6.20.3-ververica-2.0/frocksdbjni-6.20.3-ververica-2.0.jar && \
      printf '%s\n' '<?xml version="1.0" encoding="UTF-8"?>' \
        '<project xmlns="http://maven.apache.org/POM/4.0.0">' \
        '  <modelVersion>4.0.0</modelVersion>' \
        '  <groupId>com.ververica</groupId>' \
        '  <artifactId>frocksdbjni</artifactId>' \
        '  <version>6.20.3-ververica-2.0</version>' \
        '  <packaging>jar</packaging>' \
        '</project>' \
        > /root/.m2/repository/com/ververica/frocksdbjni/6.20.3-ververica-2.0/frocksdbjni-6.20.3-ververica-2.0.pom && \
      mvn clean install -T 8 -DskipTests -Dspotless.check.skip=true -Drat.skip=true -Dcheckstyle.skip; \
    fi

FROM ghcr.io/apache/flink-docker:1.18-SNAPSHOT-scala_2.12-java11-debian

RUN rm -rf /opt/flink
USER flink
ADD "https://www.random.org/cgi-bin/randbyte?nbytes=10&format=h" skipcache
COPY --from=build --chown=flink:flink /app/flink-dist/target/flink-1.18-SNAPSHOT-bin/flink-1.18-SNAPSHOT/ /opt/flink
RUN mkdir /opt/flink/examples/justin/
COPY --from=benchmarks --chown=flink:flink /app//target/* /opt/flink/examples/justin/
RUN mkdir /opt/flink/plugins/flink-s3-fs-hadoop; \
  cp /opt/flink/opt/flink-s3-fs-hadoop-1.18-SNAPSHOT.jar /opt/flink/plugins/flink-s3-fs-hadoop/
ENV FLINK_HOME=/opt/flink

# Replace default REST/RPC endpoint bind address to use the container's network interface
RUN sed -i 's/rest.address: localhost/rest.address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/rest.bind-address: localhost/rest.bind-address: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/jobmanager.bind-host: localhost/jobmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i 's/taskmanager.bind-host: localhost/taskmanager.bind-host: 0.0.0.0/g' $FLINK_HOME/conf/flink-conf.yaml; \
  sed -i '/taskmanager.host: localhost/d' $FLINK_HOME/conf/flink-conf.yaml;
