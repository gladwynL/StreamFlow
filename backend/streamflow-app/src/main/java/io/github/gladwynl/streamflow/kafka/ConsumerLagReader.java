package io.github.gladwynl.streamflow.kafka;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/** Consumer lag from committed group offsets and log-end offsets, read through the admin API. */
@Component
public class ConsumerLagReader {

    private final Admin admin;
    private final TopicInspector inspector;

    public ConsumerLagReader(Admin admin, TopicInspector inspector) {
        this.admin = admin;
        this.inspector = inspector;
    }

    /**
     * @return lag per partition; a partition without a committed offset is reported as its full log-end offset
     */
    public Map<Integer, Long> lag(String group, TopicSpec topic) {
        Map<TopicPartition, OffsetAndMetadata> committed = inspector.await(
                admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata(),
                "list offsets of group " + group);
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (int partition = 0; partition < topic.partitions(); partition++) {
            request.put(new TopicPartition(topic.name(), partition), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResultInfo> end =
                inspector.await(admin.listOffsets(request).all(), "list end offsets of " + topic.name());

        Map<Integer, Long> lag = new HashMap<>();
        end.forEach((tp, info) -> {
            OffsetAndMetadata position = committed.get(tp);
            lag.put(tp.partition(), info.offset() - (position == null ? 0 : position.offset()));
        });
        return Map.copyOf(lag);
    }

    public long totalLag(String group, TopicSpec topic) {
        return lag(group, topic).values().stream().mapToLong(Long::longValue).sum();
    }
}
