package com.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * CommitLogProducer
 *
 * CLI application that generates JSON events and sends them
 * to the Kafka "commit-log" topic in the primary cluster.
 *
 * Usage:
 *   java -jar producer.jar --count 1000
 *   java -jar producer.jar --count 1000 --bootstrap-servers localhost:9092
 */
public class CommitLogProducer {

    private static final Logger log = LoggerFactory.getLogger(CommitLogProducer.class);

    private static final String DEFAULT_TOPIC             = "commit-log";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "primary-kafka:9092";

    // Possible values for random event generation
    private static final String[] OP_TYPES    = {"INSERT", "UPDATE", "DELETE", "READ"};
    private static final String[] KEY_PREFIXES = {"doc", "user", "order", "product", "session"};
    private static final String[] STATUSES    = {"active", "archived", "pending", "deleted", "processed"};

    public static void main(String[] args) throws Exception {

        // ── Parse CLI arguments ───────────────────────────────────────────────
        int    count            = 0;
        String topic            = DEFAULT_TOPIC;
        String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--count":
                    count = Integer.parseInt(args[++i]);
                    break;
                case "--topic":
                    topic = args[++i];
                    break;
                case "--bootstrap-servers":
                    bootstrapServers = args[++i];
                    break;
                default:
                    log.warn("Unknown argument: {}", args[i]);
            }
        }

        if (count <= 0) {
            log.error("Usage: java -jar producer.jar --count <N> [--topic <topic>] [--bootstrap-servers <servers>]");
            System.exit(1);
        }

        log.info("=================================================");
        log.info("  Commit Log Producer");
        log.info("  Bootstrap Servers : {}", bootstrapServers);
        log.info("  Topic             : {}", topic);
        log.info("  Messages to Send  : {}", count);
        log.info("=================================================");

        // ── Create Kafka Producer ─────────────────────────────────────────────
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,  bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG,               "all");
        props.put(ProducerConfig.RETRIES_CONFIG,            3);
        props.put(ProducerConfig.LINGER_MS_CONFIG,          5);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG,         16384);

        int successCount = 0;
        int failCount    = 0;

        try (KafkaProducer<String, String> producer = createProducerWithRetry(props)) {

            for (int i = 0; i < count; i++) {
                String eventJson = generateEvent();
                String key       = extractKey(eventJson);

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, eventJson);

                try {
                    Future<RecordMetadata> future = producer.send(record);
                    RecordMetadata metadata = future.get(); // synchronous send for reliability
                    successCount++;

                    if ((i + 1) % 100 == 0 || i == count - 1) {
                        log.info("  Progress: {}/{} messages sent | partition={} offset={}",
                                i + 1, count, metadata.partition(), metadata.offset());
                    }

                } catch (ExecutionException | InterruptedException e) {
                    failCount++;
                    log.error("  Failed to send message {}: {}", i + 1, e.getMessage());
                }
            }

        }

        log.info("=================================================");
        log.info("  Production Complete!");
        log.info("  Success : {}", successCount);
        log.info("  Failed  : {}", failCount);
        log.info("=================================================");

        if (failCount > 0) {
            System.exit(1);
        }
    }

    // ── Helper: Create producer with retry on connection failure ──────────────
    private static KafkaProducer<String, String> createProducerWithRetry(Properties props) {
        int maxRetries = 10;
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                log.info("Connecting to Kafka... (attempt {}/{})", attempt, maxRetries);
                KafkaProducer<String, String> producer = new KafkaProducer<>(props);
                // Test connection by fetching metadata
                producer.partitionsFor((String) props.get("topic.default") != null
                        ? (String) props.get("topic.default") : DEFAULT_TOPIC);
                log.info("Connected to Kafka successfully.");
                return producer;
            } catch (Exception e) {
                log.warn("Connection attempt {} failed: {}", attempt, e.getMessage());
                if (attempt < maxRetries) {
                    try { Thread.sleep(3000); } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
                }
            }
        }
        // If all retries fail, return producer anyway (it will retry internally)
        log.warn("Could not verify connection. Creating producer anyway...");
        return new KafkaProducer<>(props);
    }

    // ── Helper: Generate a random JSON event ─────────────────────────────────
    private static String generateEvent() {
        String eventId   = UUID.randomUUID().toString();
        long   timestamp = Instant.now().getEpochSecond();
        String opType    = OP_TYPES[(int)   (Math.random() * OP_TYPES.length)];
        String keyPrefix = KEY_PREFIXES[(int)(Math.random() * KEY_PREFIXES.length)];
        String keySuffix = UUID.randomUUID().toString().substring(0, 4);
        String status    = STATUSES[(int)   (Math.random() * STATUSES.length)];

        return String.format(
            "{" +
            "\"event_id\":\"%s\"," +
            "\"timestamp\":%d," +
            "\"op_type\":\"%s\"," +
            "\"key\":\"%s:%s\"," +
            "\"value\":{\"status\":\"%s\"}" +
            "}",
            eventId, timestamp, opType, keyPrefix, keySuffix, status
        );
    }

    // ── Helper: Extract key field from JSON string ────────────────────────────
    private static String extractKey(String json) {
        // Simple extraction without external JSON library
        int start = json.indexOf("\"key\":\"") + 7;
        int end   = json.indexOf("\"", start);
        return (start > 6 && end > start) ? json.substring(start, end) : UUID.randomUUID().toString();
    }
}
