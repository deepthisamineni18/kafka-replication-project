# ── Enhanced MirrorMaker 2 Docker Image ───────────────────────────────────────
#
# Builds Apache Kafka v4.0.0 with the patched MirrorSourceTask.java
# that adds:
#   - Task 2: Log Truncation Detection (Fail-Fast)
#   - Task 3: Graceful Topic Reset Handling
#
# Build: docker build -t <your-dockerhub>/enhanced-mm2:4.0.0 .
# ─────────────────────────────────────────────────────────────────────────────

# Stage 1: Build Kafka from source with our patch
FROM gradle:8.5-jdk17 AS builder

WORKDIR /kafka-src

# Clone Apache Kafka v4.0.0
RUN apt-get update && apt-get install -y git && \
    git clone --depth 1 --branch 4.0.0 \
        https://github.com/apache/kafka.git . && \
    echo "✅ Kafka source cloned"

# Copy our patched MirrorSourceTask.java over the original
COPY src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java \
     connect/mirror/src/main/java/org/apache/kafka/connect/mirror/MirrorSourceTask.java

# Build only the connect/mirror module (faster than full build)
RUN ./gradlew :connect:mirror:jar --no-daemon -x test && \
    echo "✅ connect/mirror module built"

# Also build the full distribution for running MM2
RUN ./gradlew releaseTarGz --no-daemon -x test && \
    echo "✅ Kafka distribution built"

# Stage 2: Runtime image
FROM eclipse-temurin:17-jre-jammy

ENV KAFKA_VERSION=4.0.0
ENV KAFKA_HOME=/opt/kafka

WORKDIR /opt

# Extract the Kafka distribution
# NOTE: releaseTarGz in Kafka 4.0.0 outputs to /kafka-src/core/build/distributions/
# Use a shell variable to avoid glob-expansion bugs with tar and mv
COPY --from=builder /kafka-src/core/build/distributions/kafka_*.tgz ./
RUN TARBALL=$(ls kafka_*.tgz | grep -v site-docs | head -1) && \
    echo "Extracting: $TARBALL" && \
    tar -xzf "$TARBALL" && \
    DIRNAME=$(basename "$TARBALL" .tgz) && \
    mv "$DIRNAME" kafka && \
    rm -f kafka_*.tgz

# Replace the connect-mirror jar with our patched version
COPY --from=builder \
     /kafka-src/connect/mirror/build/libs/connect-mirror-*.jar \
     /opt/kafka/libs/

WORKDIR /opt/kafka

# Copy MirrorMaker2 config (will be mounted via docker-compose)
COPY config/mm2.properties ./config/mm2.properties

# Expose JMX port
EXPOSE 9999

# Start MirrorMaker 2
ENTRYPOINT ["bin/connect-mirror-maker.sh", "config/mm2.properties"]
