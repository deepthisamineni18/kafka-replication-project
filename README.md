# Kafka Data Replication Project

Enhanced Apache Kafka MirrorMaker 2 with fault-tolerance capabilities for mission-critical data replication.

---

## Repository Links

| Resource | Link |
|---|---|
| Kafka Fork | `https://github.com/YOUR_USERNAME/kafka` (fork of apache/kafka v4.0.0) |
| Pull Request | `https://github.com/YOUR_USERNAME/kafka/pull/1` |
| Docker Hub — MM2 | `docker pull YOUR_DOCKERHUB/enhanced-mm2:4.0.0` |
| Docker Hub — Producer | `docker pull YOUR_DOCKERHUB/commit-log-producer:1.0.0` |

> Replace `YOUR_USERNAME` and `YOUR_DOCKERHUB` after pushing.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│                                                                 │
│  ┌──────────────────┐          ┌──────────────────────────────┐ │
│  │  primary-kafka   │          │       standby-kafka          │ │
│  │  port: 9092      │          │       port: 9093             │ │
│  │                  │          │                              │ │
│  │  Topic:          │          │  Topic:                      │ │
│  │  commit-log      │          │  primary.commit-log          │ │
│  └────────┬─────────┘          └──────────────────────────────┘ │
│           │                               ▲                     │
│           │ writes                        │ replicates          │
│           │                               │                     │
│  ┌────────▼─────────┐          ┌──────────┴───────────────────┐ │
│  │    producer      │          │    Enhanced MirrorMaker 2    │ │
│  │  (Java CLI app)  │          │  + Task 2: Truncation detect │ │
│  │  --count N       │          │  + Task 3: Reset recovery    │ │
│  └──────────────────┘          └──────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
kafka-replication-project/
│
├── producer/
│   ├── src/main/java/com/kafka/producer/
│   │   └── CommitLogProducer.java     ← Java CLI event generator
│   ├── src/main/resources/
│   │   └── logback.xml
│   ├── build.gradle
│   ├── settings.gradle
│   └── Dockerfile
│
├── mirrormaker2-patch/
│   ├── src/main/java/org/apache/kafka/connect/mirror/
│   │   └── MirrorSourceTask.java      ← Modified MM2 (Task 2 + Task 3)
│   ├── config/
│   │   └── mm2.properties             ← MM2 configuration
│   └── Dockerfile
│
├── docker-compose.yml                 ← Full environment setup
├── run_challenge.sh                   ← Automated test script
└── README.md
```

---

## Setup Instructions

### Prerequisites

- Docker 24.x+
- Docker Compose v2.x+
- 4GB RAM minimum

### Step 1: Clone this repository

```bash
git clone https://github.com/YOUR_USERNAME/kafka-replication-project.git
cd kafka-replication-project
```

### Step 2: Build Docker images

```bash
# Build both custom images
docker-compose build

# Verify images were created
docker images | grep -E "enhanced-mm2|commit-log-producer"
```

### Step 3: Start the environment

```bash
# Start primary kafka, standby kafka, and mm2
docker-compose up -d primary-kafka standby-kafka
# Wait ~30 seconds for clusters to be healthy
docker-compose run --rm topic-init
docker-compose up -d mm2
```

### Step 4: Verify everything is running

```bash
docker-compose ps
```

Expected output:
```
NAME             STATUS          PORTS
primary-kafka    Up (healthy)    0.0.0.0:9092->9092/tcp
standby-kafka    Up (healthy)    0.0.0.0:9093->9093/tcp
mm2              Up              
```

---

## Test Execution

### Run All Scenarios Automatically

```bash
chmod +x run_challenge.sh
./run_challenge.sh
```

The script runs three scenarios sequentially and prints results.

### Manual Testing

#### Scenario 1 — Normal Replication

```bash
# Produce 1000 messages
docker-compose run --rm producer --count 1000 --bootstrap-servers primary-kafka:9092

# Wait 30 seconds
sleep 30

# Check message counts
# Primary
docker exec primary-kafka /opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9092 --topic commit-log --time -1

# Standby
docker exec standby-kafka /opt/kafka/bin/kafka-run-class.sh \
  kafka.tools.GetOffsetShell \
  --bootstrap-server localhost:9093 --topic primary.commit-log --time -1
```

#### Scenario 2 — Log Truncation Detection

```bash
# Pause MM2
docker-compose pause mm2

# Produce some messages
docker-compose run --rm producer --count 200 --bootstrap-servers primary-kafka:9092

# Wait 75 seconds for messages to expire (retention=60s)
sleep 75

# Resume MM2 — it should detect the gap and fail
docker-compose unpause mm2

# Watch MM2 logs for truncation detection
docker logs -f mm2 | grep -i "TRUNCATION"
```

Expected log output:
```
[ERROR] [Task2-Truncation] *** LOG TRUNCATION DETECTED — DATA LOSS! ***
  Topic-Partition        : commit-log-0
  Last Replicated Offset : 1000
  Current Begin Offset   : 1200
  Messages Lost          : 200
  Detected At            : 2024-01-15T10:30:45Z
  Action                 : Failing fast to prevent silent data loss.
```

#### Scenario 3 — Topic Reset Recovery

```bash
# Pause MM2
docker-compose pause mm2

# Delete and recreate the topic
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic commit-log
sleep 5
docker exec primary-kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic commit-log --partitions 1 --replication-factor 1 \
  --config retention.ms=60000

# Produce new messages to fresh topic
docker-compose run --rm producer --count 500 --bootstrap-servers primary-kafka:9092

# Resume MM2 — it should detect reset and recover automatically
docker-compose unpause mm2

# Watch MM2 logs for recovery
docker logs -f mm2 | grep -i "TOPIC RESET"
```

Expected log output:
```
[WARN] [Task3-Reset] *** TOPIC RESET DETECTED! ***
  Topic-Partition    : commit-log-0
  MM2 Position       : 1000
  Topic End Offset   : 500
  Detected At        : 2024-01-15T10:35:22Z
  Cause              : Topic was deleted and recreated
  Action             : Resubscribing from offset 0
[INFO] [Task3-Reset] Successfully resubscribed commit-log-0 from offset 0. Replication continues.
```

---

## Log Analysis

### Key Log Messages to Monitor

| Pattern | Scenario | Meaning |
|---|---|---|
| `LOG TRUNCATION DETECTED` | Scenario 2 | Data loss detected — MM2 failing fast |
| `Messages Lost : N` | Scenario 2 | How many messages were purged |
| `TOPIC RESET DETECTED` | Scenario 3 | Topic was deleted and recreated |
| `Resubscribing from offset 0` | Scenario 3 | MM2 auto-recovering |
| `Successfully resubscribed` | Scenario 3 | Recovery complete |

### Useful Commands

```bash
# Live MM2 logs
docker logs -f mm2

# Search for truncation events
docker logs mm2 2>&1 | grep -i "truncation"

# Search for reset events
docker logs mm2 2>&1 | grep -i "topic reset"

# Check replication lag
docker exec standby-kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9093 \
  --describe --group mm2-replication-group

# Stop everything and clean up
docker-compose down -v
```

---

## Design Rationale

### Why MirrorSourceTask.java?

MirrorMaker 2's replication pipeline has these main classes:

| Class | Role |
|---|---|
| `MirrorSourceConnector` | Manages topic discovery and task assignment |
| `MirrorSourceTask` | **Actually reads from source and writes to destination** |
| `MirrorCheckpointTask` | Syncs consumer group offsets |

`MirrorSourceTask` is where messages are consumed from the source cluster. This is the correct place to add offset validation because:
- It has direct access to the Kafka consumer
- It can call `beginningOffsets()` and `endOffsets()` efficiently
- It processes every poll cycle, enabling real-time detection

### Task 2 — Truncation Detection Logic

```
On every poll():
  for each partition:
    beginningOffset = consumer.beginningOffsets(partition)
    lastCommitted   = our tracking map

    if lastCommitted < beginningOffset:
        gap = beginningOffset - lastCommitted
        LOG ERROR with full details
        THROW RuntimeException  ← fail-fast
```

**Why fail-fast?** Silent data loss in a WAL-based system is catastrophic. Services replaying the commit-log would miss state changes. Failing loudly forces operators to investigate rather than propagating corrupt state.

### Task 3 — Topic Reset Detection Logic

```
On every poll():
  for each partition:
    currentPosition = consumer.position(partition)
    endOffset       = consumer.endOffsets(partition)

    if currentPosition > endOffset:
        LOG WARN with full details
        consumer.seek(partition, 0)    ← auto-recover
        reset tracking map entry to 0
```

**Why seek to 0?** When a topic is deleted and recreated, it starts fresh from offset 0. Seeking to 0 ensures we replicate all messages in the new topic.

**Why not fail-fast like Task 2?** Topic reset is a planned maintenance operation. Auto-recovery maintains replication continuity without operator intervention, which is the expected behavior for a resilient system.

### Minimal Disruption Design

All custom code is:
- Clearly marked with `// [CUSTOM]` comments
- Isolated in two private methods: `checkForTruncation()` and `checkForTopicReset()`
- Called only from the `poll()` method — no changes to startup/shutdown logic
- Under 100 lines of new code total

---

## AI Usage Documentation

This project used Claude (Anthropic) as an AI assistant for:

**Architecture Decisions**
- Identifying `MirrorSourceTask.java` as the correct modification point
- Designing offset comparison logic for both scenarios

**Code Generation**
- Initial structure of `CommitLogProducer.java`
- Docker Compose service configuration
- Shell script automation logic

**Review Process**
Every line of AI-generated code was:
1. Read and understood before inclusion
2. Tested against the documented behavior
3. Adjusted where the AI output didn't match Kafka's actual API signatures

The author understands all design decisions and implementation details in this codebase.

---

## Troubleshooting

| Issue | Solution |
|---|---|
| MM2 won't start | Check logs: `docker logs mm2`. Ensure both Kafka clusters are healthy first. |
| Topics not created | Run `docker-compose run --rm topic-init` manually |
| Producer can't connect | Ensure `primary-kafka` is healthy: `docker inspect primary-kafka` |
| No messages in standby | Check MM2 logs for errors. Verify `mm2.properties` cluster aliases match |
| Build fails | Ensure Docker has 4GB+ RAM allocated |
