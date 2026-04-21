# ── Stage 1: Build ────────────────────────────────────────────────────────────
FROM gradle:8.5-jdk17 AS builder

WORKDIR /app

# Copy gradle files first (for layer caching)
COPY settings.gradle build.gradle ./

# Copy source code
COPY src ./src

# Build fat jar
RUN gradle jar --no-daemon

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM eclipse-temurin:17-jre-alpine

WORKDIR /app

# Copy the built jar
COPY --from=builder /app/build/libs/producer.jar ./producer.jar

ENTRYPOINT ["java", "-jar", "/app/producer.jar"]
CMD ["--count", "1000"]
