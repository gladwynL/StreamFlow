package io.github.gladwynl.streamflow.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Gives a consumer group a committed offset on every partition that has none, set to the earliest retained offset.
 *
 * <p>The processing group runs with {@code auto.offset.reset=none} (ADR-0008): if its committed position ever falls
 * behind the log start because retention deleted unconsumed data, the consumer fails loudly instead of silently
 * skipping ahead. That setting needs every partition to have a committed offset from the start, which this
 * provides. Partitions that already have one are never touched.
 */
@Component
public class ConsumerGroupInitializer {

    private static final Logger log = LoggerFactory.getLogger(ConsumerGroupInitializer.class);

    private final Admin admin;
    private final TopicInspector inspector;

    public ConsumerGroupInitializer(Admin admin, TopicInspector inspector) {
        this.admin = admin;
        this.inspector = inspector;
    }

    /** Returns the number of partitions that received an initial offset. */
    public int initialize(String group, TopicSpec topic) {
        Map<TopicPartition, OffsetAndMetadata> committed = inspector.await(
                admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata(),
                "list offsets of group " + group);

        Map<TopicPartition, OffsetSpec> uncommitted = new HashMap<>();
        for (int partition = 0; partition < topic.partitions(); partition++) {
            TopicPartition tp = new TopicPartition(topic.name(), partition);
            if (committed.get(tp) == null) {
                uncommitted.put(tp, OffsetSpec.earliest());
            }
        }
        if (uncommitted.isEmpty()) {
            return 0;
        }

        Map<TopicPartition, ListOffsetsResultInfo> earliest =
                inspector.await(admin.listOffsets(uncommitted).all(), "list earliest offsets of " + topic.name());
        Map<TopicPartition, OffsetAndMetadata> initial = new HashMap<>();
        earliest.forEach((tp, info) -> initial.put(tp, new OffsetAndMetadata(info.offset())));
        inspector.await(admin.alterConsumerGroupOffsets(group, initial).all(), "initialize offsets of group " + group);
        log.info(
                "Initialized {} partition offsets of group {} on {} to the earliest offset",
                initial.size(),
                group,
                topic.name());
        return initial.size();
    }
}
