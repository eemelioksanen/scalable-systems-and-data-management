# syntax=docker/dockerfile:1
#
# Builds everything inside containers: no JDK or Maven needed on the host.
#   docker compose build                    # images for flink-job, producer and viz
#   docker build --target test .            # run the Maven test suite
#
# Keep FLINK_IMAGE in sync with <flink.version> in pom.xml.
ARG FLINK_IMAGE=flink:2.2.1-java17

# Dashboard frontend: Vitest runs here, and the built files are served from the viz jar.
FROM node:24-alpine AS viz-web
WORKDIR /web
COPY viz/web/package.json viz/web/package-lock.json ./
RUN --mount=type=cache,target=/root/.npm npm ci --no-audit --no-fund
COPY viz/web/ ./
RUN npm test && npm run build

FROM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
COPY producer/ producer/
COPY flink-job/ flink-job/
COPY viz/pom.xml viz/pom.xml
COPY viz/src/ viz/src/
COPY --from=viz-web /web/dist/ viz/src/main/resources/static/
# The cache mount keeps ~/.m2 across builds, so dependencies are downloaded once.
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp package -DskipTests

FROM build AS test
# Hand-worked EMA sequences used by GoldenFixturesTest.
COPY fixtures/golden/ fixtures/golden/
RUN --mount=type=cache,target=/root/.m2 mvn -B -ntp clean verify

# Flink image with the job jar baked in; used for both JobManager and TaskManagers.
FROM ${FLINK_IMAGE} AS flink-job
COPY --from=build /src/flink-job/target/flink-job.jar /opt/job/flink-job.jar

# Kafka with the Prometheus JMX exporter agent; compose passes the agent to the broker only.
FROM apache/kafka:4.3.1 AS kafka
ADD --chmod=644 --checksum=sha256:0ad843261c567d1924a017d542e14c40f2afff523d940c25ee000742fde4b3a9 \
    https://github.com/prometheus/jmx_exporter/releases/download/1.7.0/jmx_prometheus_javaagent-1.7.0.jar \
    /opt/jmx-exporter/jmx_prometheus_javaagent.jar

FROM eclipse-temurin:17-jre AS producer
COPY --from=build /src/producer/target/producer.jar /app/producer.jar
ENTRYPOINT ["java", "-cp", "/app/producer.jar"]

FROM eclipse-temurin:17-jre AS viz
COPY --from=build /src/viz/target/viz.jar /app/viz.jar
EXPOSE 8088
ENTRYPOINT ["java", "-Xmx512m", "-jar", "/app/viz.jar"]
