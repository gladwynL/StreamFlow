package io.github.gladwynl.streamflow.kafka;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.stereotype.Component;

/**
 * A consumer group as Kafka reports it: state, members and their partition assignments, and per-partition committed
 * offset, log-end offset and lag. Ownership comes from the group coordinator, not from the workers.
 */
@Component
public class ConsumerGroupInspector {

    public record Member(String memberId, String clientId, String host, List<Integer> partitions) {}

    /**
     * @param committedOffset null when the group has no committed offset for the partition
     * @param ownerClientId null when no member currently owns the partition (for example during a rebalance)
     */
    public record PartitionState(
            int partition, long logEndOffset, Long committedOffset, long lag, String ownerClientId) {}

    public record GroupState(
            String groupId, String topic, String state, List<Member> members, List<PartitionState> partitions) {

        public long totalLag() {
            return partitions.stream().mapToLong(PartitionState::lag).sum();
        }

        public long logEndOffset() {
            return partitions.stream().mapToLong(PartitionState::logEndOffset).sum();
        }
    }

    private final Admin admin;
    private final TopicInspector inspector;

    public ConsumerGroupInspector(Admin admin, TopicInspector inspector) {
        this.admin = admin;
        this.inspector = inspector;
    }

    /**
     * Bytes stored on the broker(s) for each partition of {@code topic} (all log segments). Compared with the topic's
     * {@code retention.bytes}, this is how close Kafka is to deleting a partition's oldest records (Phase 0 §6.2).
     */
    public Map<Integer, Long> partitionSizes(TopicSpec topic) {
        List<Integer> brokers = inspector.await(admin.describeCluster().nodes(), "describe cluster").stream()
                .map(org.apache.kafka.common.Node::id)
                .toList();
        Map<Integer, Long> sizes = new HashMap<>();
        inspector
                .await(admin.describeLogDirs(brokers).allDescriptions(), "describe log dirs")
                .values()
                .forEach(dirs -> dirs.values()
                        .forEach(dir -> dir.replicaInfos().forEach((partition, info) -> {
                            if (partition.topic().equals(topic.name())) {
                                sizes.merge(partition.partition(), info.size(), Math::max);
                            }
                        })));
        return sizes;
    }

    /** The group's committed offsets on every partition it has one for. */
    public Map<TopicPartition, Long> committedOffsets(String groupId) {
        Map<TopicPartition, OffsetAndMetadata> committed = inspector.await(
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "list offsets of group " + groupId);
        Map<TopicPartition, Long> out = new HashMap<>();
        committed.forEach((partition, offset) -> {
            if (offset != null) {
                out.put(partition, offset.offset());
            }
        });
        return out;
    }

    public GroupState describe(String groupId, TopicSpec topic) {
        ConsumerGroupDescription description = inspector
                .await(admin.describeConsumerGroups(List.of(groupId)).all(), "describe group " + groupId)
                .get(groupId);
        Map<TopicPartition, OffsetAndMetadata> committed = inspector.await(
                admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata(),
                "list offsets of group " + groupId);
        Map<TopicPartition, OffsetSpec> request = new HashMap<>();
        for (int p = 0; p < topic.partitions(); p++) {
            request.put(new TopicPartition(topic.name(), p), OffsetSpec.latest());
        }
        Map<TopicPartition, ListOffsetsResultInfo> end =
                inspector.await(admin.listOffsets(request).all(), "list end offsets of " + topic.name());

        List<Member> members = new ArrayList<>();
        Map<Integer, String> owners = new HashMap<>();
        if (description != null) {
            for (MemberDescription m : description.members()) {
                List<Integer> partitions = m.assignment().topicPartitions().stream()
                        .filter(tp -> tp.topic().equals(topic.name()))
                        .map(TopicPartition::partition)
                        .sorted()
                        .toList();
                partitions.forEach(p -> owners.put(p, m.clientId()));
                members.add(new Member(m.consumerId(), m.clientId(), m.host(), partitions));
            }
        }
        members.sort(Comparator.comparing(Member::clientId));

        List<PartitionState> partitions = new ArrayList<>();
        for (int p = 0; p < topic.partitions(); p++) {
            TopicPartition tp = new TopicPartition(topic.name(), p);
            long logEnd = end.get(tp).offset();
            OffsetAndMetadata position = committed.get(tp);
            Long committedOffset = position == null ? null : position.offset();
            long lag = Math.max(0, logEnd - (committedOffset == null ? 0 : committedOffset));
            partitions.add(new PartitionState(p, logEnd, committedOffset, lag, owners.get(p)));
        }
        String state =
                description == null ? "UNKNOWN" : description.groupState().toString();
        return new GroupState(groupId, topic.name(), state, List.copyOf(members), List.copyOf(partitions));
    }
}
