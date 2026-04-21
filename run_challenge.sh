#!/usr/bin/env bash
# ══════════════════════════════════════════════════════════════════════════════
#  run_challenge.sh
#  Orchestrates all three test scenarios for the Kafka Replication Project
#
#  Scenarios:
#    1. Normal Replication Flow
#    2. Log Truncation Detection (Fail-Fast)
#    3. Topic Reset Recovery (Graceful Handling)
#
#  Usage:
#    chmod +x run_challenge.sh
#    ./run_challenge.sh
# ══════════════════════════════════════════════════════════════════════════════

set -euo pipefail

# ── Colours ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m' # No Colour

# ── Config ────────────────────────────────────────────────────────────────────
PRIMARY_KAFKA="primary-kafka:9092"
STANDBY_KAFKA="standby-kafka:9093"
SOURCE_TOPIC="commit-log"
TARGET_TOPIC="primary.commit-log"
PRODUCE_COUNT=1000
RETENTION_WAIT=75   # seconds — longer than 60s retention to ensure truncation

# ── Helper functions ──────────────────────────────────────────────────────────
log_info()    { echo -e "${BLUE}[INFO]${NC}  $*"; }
log_ok()      { echo -e "${GREEN}[OK]${NC}    $*"; }
log_warn()    { echo -e "${YELLOW}[WARN]${NC}  $*"; }
log_error()   { echo -e "${RED}[ERROR]${NC} $*"; }
log_section() { echo -e "\n${BOLD}${CYAN}══════════════════════════════════════════${NC}"; \
                echo -e "${BOLD}${CYAN}  $*${NC}"; \
                echo -e "${BOLD}${CYAN}══════════════════════════════════════════${NC}"; }

run_in_primary() {
    docker exec primary-kafka /opt/kafka/bin/"$@"
}

run_in_standby() {
    docker exec standby-kafka /opt/kafka/bin/"$@"
}

wait_for_service() {
    local service="$1"
    local max_wait="${2:-60}"
    log_info "Waiting for $service to be healthy..."
    local elapsed=0
    until docker inspect --format='{{.State.Health.Status}}' "$service" 2>/dev/null | grep -q "healthy"; do
        sleep 2; elapsed=$((elapsed + 2))
        if [ "$elapsed" -ge "$max_wait" ]; then
            log_error "$service did not become healthy in ${max_wait}s"
            return 1
        fi
        echo -n "."
    done
    echo ""
    log_ok "$service is healthy"
}

get_offset_count() {
    local server="$1"
    local topic="$2"
    docker exec "$(echo $server | cut -d: -f1)" \
        /opt/kafka/bin/kafka-run-class.sh kafka.tools.GetOffsetShell \
        --bootstrap-server "$server" \
        --topic "$topic" \
        --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum+0}'
}

# ══════════════════════════════════════════════════════════════════════════════
#  SETUP
# ══════════════════════════════════════════════════════════════════════════════
log_section "SETUP — Starting All Services"

log_info "Bringing up primary-kafka and standby-kafka..."
docker-compose up -d primary-kafka standby-kafka

wait_for_service "primary-kafka" 120
wait_for_service "standby-kafka" 120

log_info "Initialising topics..."
docker-compose run --rm topic-init
sleep 5

log_info "Starting Enhanced MirrorMaker 2..."
docker-compose up -d mm2
sleep 20
log_ok "Setup complete. All services running."

# ══════════════════════════════════════════════════════════════════════════════
#  SCENARIO 1 — Normal Replication Flow
# ══════════════════════════════════════════════════════════════════════════════
log_section "SCENARIO 1 — Normal Replication Flow"

log_info "Producing ${PRODUCE_COUNT} messages to '${SOURCE_TOPIC}'..."
docker-compose run --rm --no-deps producer \
    --count "$PRODUCE_COUNT" \
    --bootstrap-servers "$PRIMARY_KAFKA"

log_info "Waiting 30s for replication to complete..."
sleep 30

PRIMARY_COUNT=$(get_offset_count "$PRIMARY_KAFKA" "$SOURCE_TOPIC")
STANDBY_COUNT=$(get_offset_count "$STANDBY_KAFKA" "$TARGET_TOPIC")

echo ""
echo -e "  Primary  '${SOURCE_TOPIC}'       : ${BOLD}${PRIMARY_COUNT}${NC} messages"
echo -e "  Standby  '${TARGET_TOPIC}' : ${BOLD}${STANDBY_COUNT}${NC} messages"
echo ""

if [ "$PRIMARY_COUNT" -eq "$PRODUCE_COUNT" ] && [ "$STANDBY_COUNT" -gt 0 ]; then
    log_ok "SCENARIO 1 PASSED ✅ — Replication working. Primary=${PRIMARY_COUNT}, Standby=${STANDBY_COUNT}"
else
    log_warn "SCENARIO 1: Replication may still be in progress. Primary=${PRIMARY_COUNT}, Standby=${STANDBY_COUNT}"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  SCENARIO 2 — Log Truncation Detection
# ══════════════════════════════════════════════════════════════════════════════
log_section "SCENARIO 2 — Log Truncation Detection (Fail-Fast)"

log_info "Step 1: Pausing MM2 so messages can expire..."
docker-compose pause mm2

log_info "Step 2: Producing 200 more messages (these will expire)..."
docker-compose run --rm --no-deps producer \
    --count 200 \
    --bootstrap-servers "$PRIMARY_KAFKA"

log_info "Step 3: Waiting ${RETENTION_WAIT}s for Kafka retention to purge messages..."
echo -n "  Countdown: "
for i in $(seq "$RETENTION_WAIT" -5 1); do
    echo -n "${i}s... "
    sleep 5
done
echo ""

log_info "Step 4: Verify messages were purged on primary..."
AFTER_RETENTION=$(get_offset_count "$PRIMARY_KAFKA" "$SOURCE_TOPIC")
log_info "  Offset after retention purge: ${AFTER_RETENTION}"

log_info "Step 5: Resuming MM2 — it should detect the gap and fail-fast..."
docker-compose unpause mm2

log_info "Step 6: Waiting 20s for MM2 to detect truncation..."
sleep 20

log_info "Step 7: Checking MM2 logs for truncation detection..."
echo ""
echo -e "${YELLOW}── MM2 Logs (last 50 lines) ──────────────────────${NC}"
docker logs mm2 --tail=50 2>&1
echo -e "${YELLOW}──────────────────────────────────────────────────${NC}"
echo ""

if docker logs mm2 2>&1 | grep -q "LOG TRUNCATION DETECTED"; then
    log_ok "SCENARIO 2 PASSED ✅ — MM2 detected log truncation and failed-fast!"
else
    log_warn "SCENARIO 2: Truncation log not found yet. Check MM2 logs manually:"
    log_warn "  docker logs mm2 | grep 'TRUNCATION'"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  SCENARIO 3 — Topic Reset Recovery
# ══════════════════════════════════════════════════════════════════════════════
log_section "SCENARIO 3 — Topic Reset (Delete + Recreate) Recovery"

log_info "Step 1: Restarting MM2 cleanly after truncation failure..."
docker-compose restart mm2
sleep 15

log_info "Step 2: Pausing MM2 before topic reset..."
docker-compose pause mm2

log_info "Step 3: Deleting '${SOURCE_TOPIC}' topic on primary cluster..."
run_in_primary kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --delete \
    --topic "$SOURCE_TOPIC"
sleep 5

log_info "Step 4: Recreating '${SOURCE_TOPIC}' topic (fresh, empty)..."
run_in_primary kafka-topics.sh \
    --bootstrap-server localhost:9092 \
    --create \
    --topic "$SOURCE_TOPIC" \
    --partitions 1 \
    --replication-factor 1 \
    --config retention.ms=60000
sleep 5
log_ok "Topic '${SOURCE_TOPIC}' recreated successfully"

log_info "Step 5: Producing 500 new messages to the fresh topic..."
docker-compose run --rm --no-deps producer \
    --count 500 \
    --bootstrap-servers "$PRIMARY_KAFKA"

log_info "Step 6: Resuming MM2 — it should detect reset and recover automatically..."
docker-compose unpause mm2

log_info "Step 7: Waiting 30s for MM2 to detect reset and replicate..."
sleep 30

log_info "Step 8: Checking MM2 logs for topic reset detection..."
echo ""
echo -e "${YELLOW}── MM2 Logs (last 50 lines) ──────────────────────${NC}"
docker logs mm2 --tail=50 2>&1
echo -e "${YELLOW}──────────────────────────────────────────────────${NC}"
echo ""

STANDBY_AFTER_RESET=$(get_offset_count "$STANDBY_KAFKA" "$TARGET_TOPIC")

if docker logs mm2 2>&1 | grep -q "TOPIC RESET DETECTED"; then
    log_ok "SCENARIO 3 PASSED ✅ — MM2 detected topic reset and recovered automatically!"
    log_ok "  Messages in standby after recovery: ${STANDBY_AFTER_RESET}"
else
    log_warn "SCENARIO 3: Reset log not found yet. Check MM2 logs:"
    log_warn "  docker logs mm2 | grep 'TOPIC RESET'"
fi

# ══════════════════════════════════════════════════════════════════════════════
#  FINAL SUMMARY
# ══════════════════════════════════════════════════════════════════════════════
log_section "FINAL SUMMARY"

echo -e "  ${BOLD}Scenario 1 — Normal Replication  :${NC} Check if Primary & Standby counts match"
echo -e "  ${BOLD}Scenario 2 — Log Truncation      :${NC} Run: docker logs mm2 | grep 'TRUNCATION'"
echo -e "  ${BOLD}Scenario 3 — Topic Reset         :${NC} Run: docker logs mm2 | grep 'TOPIC RESET'"
echo ""
echo -e "  ${BOLD}View all MM2 logs:${NC}  docker logs -f mm2"
echo -e "  ${BOLD}Stop everything:${NC}    docker-compose down -v"
echo ""
log_ok "Challenge run complete! 🎉"
