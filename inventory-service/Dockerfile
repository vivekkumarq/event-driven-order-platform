# syntax=docker/dockerfile:1
#
# Multi-stage build for inventory-service.
# Build context is this directory: docker build -t inventory-service ./inventory-service
#
# NOTE: Docker was not available in the environment this file was written in, so it has not
# been built or run. Treat it as reviewed-but-unverified.

# ---------- build ----------
FROM maven:3.9.9-eclipse-temurin-17 AS build
WORKDIR /build

# Resolve dependencies in their own layer so a source-only change does not re-download the world.
COPY pom.xml ./
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -DskipTests package \
    && mv target/*.jar target/app.jar

# ---------- runtime ----------
FROM eclipse-temurin:17-jre-alpine AS runtime

# BusyBox wget is used by the healthcheck below and ships with the base image.
RUN addgroup -S app && adduser -S -G app app

WORKDIR /app
COPY --from=build --chown=app:app /build/target/app.jar ./app.jar

USER app
EXPOSE 8082

ENV JAVA_OPTS="-XX:MaxRAMPercentage=75.0"
ENV SERVER_PORT=8082

HEALTHCHECK --interval=15s --timeout=5s --start-period=60s --retries=5 \
    CMD wget -q -O - "http://127.0.0.1:${SERVER_PORT}/actuator/health" | grep -q '"status":"UP"' || exit 1

ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar /app/app.jar"]
