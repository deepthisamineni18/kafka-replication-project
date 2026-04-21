/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.header.ConnectHeaders;
import org.apache.kafka.connect.header.Headers;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/**
 * Replicates a set of topic-partitions.
 *
 * Enhanced with two fault-tolerance features (all custom code marked [CUSTOM]):
 *
 *  Task 2 - Log Truncation Detection (Fail-Fast)
 *    Detects when Kafka retention purged messages before MM2 replicated them.
 *    On detection: logs detailed error + throws RuntimeException (fail-fast).
 *
 *  Task 3 - Graceful Topic Reset Handling
 *    Detects when source topic was deleted and recreated (offsets reset to 0).
 *    On detection: logs the event + auto-seeks to offset 0 to continue replication.
 */
public class MirrorSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(MirrorSourceTask.class);

    // [CUSTOM] Any gap > 0 between MM2's position and topic beginning = data loss
    private static final long TRUNCATION_GAP_THRESHOLD = 0L;

    private KafkaConsumer<byte[], byte[]> consumer;
    private String sourceClusterAlias;
    private Duration pollTimeout;
    private ReplicationPolicy replicationPolicy;
    private MirrorSourceMetrics metrics;
    private boolean stopping = false;
    private Semaphore consumerAccess;
    private OffsetSyncWriter offsetSyncWriter;

    // [CUSTOM] Tracks next-expected offset per partition for fault detection
    private final Map<TopicPartition, Long> lastLoadedOffsets = new HashMap<>();

    public MirrorSourceTask() {}

    // for testing
    MirrorSourceTask(KafkaConsumer<byte[], byte[]> consumer, MirrorSourceMetrics metrics,
                     String sourceClusterAlias, ReplicationPolicy replicationPolicy,
                     OffsetSyncWriter offsetSyncWriter) {
        this.consumer = consumer;
        this.metrics = metrics;
        this.sourceClusterAlias = sourceClusterAlias;
        this.replicationPolicy = replicationPolicy;
        consumerAccess = new Semaphore(1);
        this.offsetSyncWriter = offsetSyncWriter;
    }

    @Override
    public void start(Map<String, String> props) {
        MirrorSourceTaskConfig config = new MirrorSourceTaskConfig(props);
        consumerAccess = new Semaphore(1);
        sourceClusterAlias = config.sourceClusterAlias();
        metrics = config.metrics();
        pollTimeout = config.consumerPollTimeout();
        replicationPolicy = config.replicationPolicy();
        if (config.emitOffsetSyncsEnabled()) {
            offsetSyncWriter = new OffsetSyncWriter(config);
        }
        consumer = MirrorUtils.newConsumer(config.sourceConsumerConfig("replication-consumer"));
        Set<TopicPartition> taskTopicPartitions = config.taskTopicPartitions();
        initializeConsumer(taskTopicPartitions);

        log.info("{} replicating {} topic-partitions {}->{}: {}.",
                Thread.currentThread().getName(), taskTopicPartitions.size(),
                sourceClusterAlias, config.targetClusterAlias(), taskTopicPartitions);
    }

    @Override
    public void commit() {
        if (offsetSyncWriter != null) {
            offsetSyncWriter.promoteDelayedOffsetSyncs();
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    @Override
    public void stop() {
        long start = System.currentTimeMillis();
        stopping = true;
        consumer.wakeup();
        try {
            consumerAccess.acquire();
        } catch (InterruptedException e) {
            log.warn("Interrupted waiting for access to consumer. Will try closing anyway.");
        }
        Utils.closeQuietly(consumer, "source consumer");
        Utils.closeQuietly(offsetSyncWriter, "offset sync writer");
        Utils.closeQuietly(metrics, "metrics");
        log.info("Stopping {} took {} ms.", Thread.currentThread().getName(),
                System.currentTimeMillis() - start);
    }

    @Override
    public String version() {
        return new MirrorSourceConnector().version();
    }

    @Override
    public List<SourceRecord> poll() {
        if (!consumerAccess.tryAcquire()) {
            return null;
        }
        if (stopping) {
            return null;
        }
        try {
            // [CUSTOM] Run fault-tolerance checks before every poll
            for (TopicPartition tp : consumer.assignment()) {
                checkForTruncation(tp);  // Task 2
                checkForTopicReset(tp);  // Task 3
            }

            ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
            List<SourceRecord> sourceRecords = new ArrayList<>(records.count());

            for (ConsumerRecord<byte[], byte[]> record : records) {
                SourceRecord converted = convertRecord(record);
                sourceRecords.add(converted);
                TopicPartition topicPartition = new TopicPartition(converted.topic(), converted.kafkaPartition());
                metrics.recordAge(topicPartition, System.currentTimeMillis() - record.timestamp());
                metrics.recordBytes(topicPartition, byteSize(record.value()));

                // [CUSTOM] Update offset tracking: store offset+1 (next expected)
                TopicPartition sourceTp = new TopicPartition(record.topic(), record.partition());
                lastLoadedOffsets.put(sourceTp, record.offset() + 1L);
            }

            if (sourceRecords.isEmpty()) {
                return null;
            } else {
                log.trace("Polled {} records from {}.", sourceRecords.size(), records.partitions());
                return sourceRecords;
            }
        } catch (WakeupException e) {
            return null;
        } catch (KafkaException e) {
            log.warn("Failure during poll.", e);
            return null;
        } catch (Throwable e) {
            log.error("Failure during poll.", e);
            throw e;
        } finally {
            consumerAccess.release();
        }
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) {
        if (stopping) {
            return;
        }
        if (metadata == null) {
            log.debug("No RecordMetadata (source record was probably filtered out) -- can't sync offsets for {}.",
                    record.topic());
            return;
        }
        if (!metadata.hasOffset()) {
            log.error("RecordMetadata has no offset -- can't sync offsets for {}.", record.topic());
            return;
        }
        TopicPartition topicPartition = new TopicPartition(record.topic(), record.kafkaPartition());
        long latency = System.currentTimeMillis() - record.timestamp();
        metrics.countRecord(topicPartition);
        metrics.replicationLatency(topicPartition, latency);
        if (offsetSyncWriter != null) {
            TopicPartition sourceTopicPartition = MirrorUtils.unwrapPartition(record.sourcePartition());
            long upstreamOffset = MirrorUtils.unwrapOffset(record.sourceOffset());
            long downstreamOffset = metadata.offset();
            offsetSyncWriter.maybeQueueOffsetSyncs(sourceTopicPartition, upstreamOffset, downstreamOffset);
            offsetSyncWriter.firePendingOffsetSyncs();
        }
    }

    // ==========================================================================
    // [CUSTOM] Task 2 - Log Truncation Detection (Fail-Fast)
    // ==========================================================================

    /**
     * Checks if Kafka retention has purged messages that MM2 hasn't replicated yet.
     *
     * How it works:
     *   lastLoadedOffset = next offset MM2 expects to read (our bookmark)
     *   beginningOffset  = earliest offset currently in the source topic
     *
     *   If beginningOffset > lastLoadedOffset => gap exists => data was lost
     *   => Log detailed error and throw RuntimeException to halt replication
     */
    private void checkForTruncation(TopicPartition tp) {
        Long lastOffset = lastLoadedOffsets.get(tp);
        if (lastOffset == null) {
            return; // First run, no previous offset to compare
        }

        Map<TopicPartition, Long> beginningOffsets =
                consumer.beginningOffsets(Collections.singletonList(tp));
        Long beginningOffset = beginningOffsets.get(tp);

        if (beginningOffset == null) {
            log.warn("[Task2-Truncation] Could not fetch beginning offset for {}. Skipping check.", tp);
            return;
        }

        long gap = beginningOffset - lastOffset;

        if (gap > TRUNCATION_GAP_THRESHOLD) {
            log.error(
                "[Task2-Truncation] *** LOG TRUNCATION DETECTED - DATA LOSS ***\n" +
                "  Topic-Partition        : {}\n" +
                "  MM2 Expected Offset    : {}\n" +
                "  Topic Beginning Offset : {}\n" +
                "  Messages Lost          : {}\n" +
                "  Detected At            : {}\n" +
                "  Action                 : Failing fast to prevent silent data loss.",
                tp, lastOffset, beginningOffset, gap, Instant.now()
            );
            throw new RuntimeException(String.format(
                "Log truncation detected on %s: MM2 expected offset %d but topic begins at %d. " +
                "%d messages were lost due to retention policy.",
                tp, lastOffset, beginningOffset, gap
            ));
        }

        log.debug("[Task2-Truncation] OK. partition={} lastOffset={} beginningOffset={}",
                tp, lastOffset, beginningOffset);
    }

    // ==========================================================================
    // [CUSTOM] Task 3 - Graceful Topic Reset Handling
    // ==========================================================================

    /**
     * Detects when source topic was deleted and recreated (topic reset).
     *
     * How it works:
     *   currentPosition = where consumer will read next
     *   endOffset       = latest offset currently in the topic
     *
     *   If currentPosition > endOffset => topic was recreated with fewer messages
     *   => Log the reset event and seek to offset 0 to resume replication
     */
    private void checkForTopicReset(TopicPartition tp) {
        Long lastOffset = lastLoadedOffsets.get(tp);
        if (lastOffset == null) {
            return; // First run, skip
        }

        long currentPosition;
        try {
            currentPosition = consumer.position(tp);
        } catch (Exception e) {
            log.warn("[Task3-Reset] Could not get consumer position for {}: {}", tp, e.getMessage());
            return;
        }

        Map<TopicPartition, Long> endOffsets =
                consumer.endOffsets(Collections.singletonList(tp));
        Long endOffset = endOffsets.get(tp);

        if (endOffset == null) {
            log.warn("[Task3-Reset] Could not fetch end offset for {}. Skipping check.", tp);
            return;
        }

        if (currentPosition > endOffset) {
            log.warn(
                "[Task3-Reset] *** TOPIC RESET DETECTED ***\n" +
                "  Topic-Partition   : {}\n" +
                "  Consumer Position : {} (where MM2 expected to read next)\n" +
                "  Topic End Offset  : {} (topic has fewer messages than expected)\n" +
                "  Detected At       : {}\n" +
                "  Cause             : Source topic was deleted and recreated\n" +
                "  Action            : Resubscribing from offset 0",
                tp, currentPosition, endOffset, Instant.now()
            );

            // Auto-recover: seek to beginning of the new (recreated) topic
            consumer.seek(tp, 0L);

            // Reset tracking so truncation check doesn't misfire
            lastLoadedOffsets.put(tp, 0L);

            log.info("[Task3-Reset] Successfully resubscribed {} from offset 0. Replication continues.", tp);
        } else {
            log.debug("[Task3-Reset] OK. partition={} position={} endOffset={}",
                    tp, currentPosition, endOffset);
        }
    }

    // ==========================================================================
    // Standard MM2 helper methods (unchanged from vanilla Kafka v4.0.0)
    // ==========================================================================

    private Map<TopicPartition, Long> loadOffsets(Set<TopicPartition> topicPartitions) {
        return topicPartitions.stream().collect(Collectors.toMap(x -> x, this::loadOffset));
    }

    private Long loadOffset(TopicPartition topicPartition) {
        Map<String, Object> wrappedPartition = MirrorUtils.wrapPartition(topicPartition, sourceClusterAlias);
        Map<String, Object> wrappedOffset = context.offsetStorageReader().offset(wrappedPartition);
        return MirrorUtils.unwrapOffset(wrappedOffset);
    }

    void initializeConsumer(Set<TopicPartition> taskTopicPartitions) {
        Map<TopicPartition, Long> topicPartitionOffsets = loadOffsets(taskTopicPartitions);
        consumer.assign(topicPartitionOffsets.keySet());
        log.info("Starting with {} previously uncommitted partitions.",
                topicPartitionOffsets.values().stream().filter(this::isUncommitted).count());

        topicPartitionOffsets.forEach((topicPartition, offset) -> {
            if (isUncommitted(offset)) {
                log.trace("Skipping seeking offset for topicPartition: {}", topicPartition);
                return;
            }
            long nextOffset = offset + 1L;
            log.trace("Seeking to offset {} for topicPartition: {}", nextOffset, topicPartition);
            consumer.seek(topicPartition, nextOffset);
            lastLoadedOffsets.put(topicPartition, nextOffset); // [CUSTOM] init tracking
        });
    }

    SourceRecord convertRecord(ConsumerRecord<byte[], byte[]> record) {
        String targetTopic = formatRemoteTopic(record.topic());
        Headers headers = convertHeaders(record);
        return new SourceRecord(
                MirrorUtils.wrapPartition(new TopicPartition(record.topic(), record.partition()), sourceClusterAlias),
                MirrorUtils.wrapOffset(record.offset()),
                targetTopic, record.partition(),
                Schema.OPTIONAL_BYTES_SCHEMA, record.key(),
                Schema.BYTES_SCHEMA, record.value(),
                record.timestamp(), headers);
    }

    private Headers convertHeaders(ConsumerRecord<byte[], byte[]> record) {
        ConnectHeaders headers = new ConnectHeaders();
        for (Header header : record.headers()) {
            headers.addBytes(header.key(), header.value());
        }
        return headers;
    }

    private String formatRemoteTopic(String topic) {
        return replicationPolicy.formatRemoteTopic(sourceClusterAlias, topic);
    }

    private static int byteSize(byte[] bytes) {
        if (bytes == null) {
            return 0;
        }
        return bytes.length;
    }

    private boolean isUncommitted(Long offset) {
        return offset == null || offset < 0;
    }
}
