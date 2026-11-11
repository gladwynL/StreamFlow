package io.github.gladwynl.streamflow.deadletter;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.gladwynl.streamflow.events.EventHeaders;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.NewDeadLetter;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.time.Instant;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;
import tools.jackson.databind.json.JsonMapper;

/**
 * Copies one dead-letter record into {@code dead_letter_events}, preserving the original topic, partition, offset,
 * key, value bytes, event metadata and failure reason. Idempotent: the source coordinates are unique, so a dead
 * letter published twice for the same source record is stored once.
 */
@Component
public class DeadLetterIngestion {

    static final int MAX_MESSAGE_LENGTH = 2_000;
    private static final Set<String> BINARY_HEADERS = Set.of(
            KafkaHeaders.DLT_ORIGINAL_PARTITION,
            KafkaHeaders.DLT_ORIGINAL_OFFSET,
            KafkaHeaders.DLT_ORIGINAL_TIMESTAMP,
            KafkaHeaders.DLT_ORIGINAL_TIMESTAMP_TYPE);
    private static final String STACKTRACE = KafkaHeaders.DLT_EXCEPTION_STACKTRACE;

    private final DeadLetterRepository repository;
    private final JsonMapper json = JsonMapper.builder().build();

    public DeadLetterIngestion(DeadLetterRepository repository) {
        this.repository = repository;
    }

    /** Returns {@code true} if a new dead letter was stored, {@code false} if it was already present. */
    public boolean ingest(ConsumerRecord<String, byte[]> record) {
        String sourceTopic = text(record, KafkaHeaders.DLT_ORIGINAL_TOPIC);
        Integer sourcePartition = intHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION);
        Long sourceOffset = longHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET);
        if (sourceTopic == null || sourcePartition == null || sourceOffset == null) {
            // Not produced by our recoverer. Store it under its dead-letter coordinates so it is never lost.
            sourceTopic = record.topic();
            sourcePartition = record.partition();
            sourceOffset = record.offset();
        }
        Long sourceTimestamp = longHeader(record, KafkaHeaders.DLT_ORIGINAL_TIMESTAMP);
        String category = text(record, DeadLetterHeaders.FAILURE_CATEGORY);
        String attempts = text(record, DeadLetterHeaders.DELIVERY_ATTEMPTS);
        String sequence = text(record, EventHeaders.SEQUENCE);

        NewDeadLetter deadLetter = new NewDeadLetter(
                sourceTopic,
                sourcePartition,
                sourceOffset,
                sourceTimestamp == null ? null : Instant.ofEpochMilli(sourceTimestamp),
                record.partition(),
                record.offset(),
                uuid(text(record, EventHeaders.EVENT_ID)),
                uuid(text(record, EventHeaders.RUN_ID)),
                parseLong(sequence),
                record.key(),
                record.value(),
                headersJson(record),
                category == null ? "UNKNOWN" : category,
                text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN) != null
                        ? text(record, KafkaHeaders.DLT_EXCEPTION_CAUSE_FQCN)
                        : text(record, KafkaHeaders.DLT_EXCEPTION_FQCN),
                truncate(text(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE)),
                parseLong(attempts) == null ? null : parseLong(attempts).intValue(),
                Instant.ofEpochMilli(record.timestamp()));
        return repository.insertIfAbsent(deadLetter);
    }

    /** Headers as a JSON object; binary Spring headers decoded to numbers, stack traces left out. */
    private String headersJson(ConsumerRecord<String, byte[]> record) {
        Map<String, Object> headers = new LinkedHashMap<>();
        for (Header header : record.headers()) {
            String key = header.key();
            if (key.equals(STACKTRACE) || header.value() == null) {
                continue;
            }
            if (BINARY_HEADERS.contains(key)) {
                headers.put(
                        key,
                        header.value().length == 4
                                ? ByteBuffer.wrap(header.value()).getInt()
                                : header.value().length == 8
                                        ? ByteBuffer.wrap(header.value()).getLong()
                                        : Base64.getEncoder().encodeToString(header.value()));
            } else {
                String text = printable(header.value());
                headers.put(
                        key,
                        text != null
                                ? truncate(text)
                                : "base64:" + Base64.getEncoder().encodeToString(header.value()));
            }
        }
        return json.writeValueAsString(headers);
    }

    /**
     * A header as text, or null if it is not printable UTF-8 (for example Spring's binary
     * {@code kafka_deliveryAttempt}). PostgreSQL rejects NUL in {@code text} and {@code jsonb}, so binary values must
     * never be stored as strings.
     */
    static String printable(byte[] value) {
        String text;
        try {
            text = UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value))
                    .toString();
        } catch (CharacterCodingException e) {
            return null;
        }
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isISOControl(c) && c != '\n' && c != '\r' && c != '\t') {
                return null;
            }
        }
        return text;
    }

    /** A header as text with any characters PostgreSQL text cannot hold removed; null if absent. */
    private static String text(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null || header.value() == null
                ? null
                : new String(header.value(), UTF_8).replace("\u0000", "");
    }

    private static Integer intHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null || header.value() == null || header.value().length != 4
                ? null
                : ByteBuffer.wrap(header.value()).getInt();
    }

    private static Long longHeader(ConsumerRecord<?, ?> record, String key) {
        Header header = record.headers().lastHeader(key);
        return header == null || header.value() == null || header.value().length != 8
                ? null
                : ByteBuffer.wrap(header.value()).getLong();
    }

    private static UUID uuid(String value) {
        try {
            return value == null ? null : UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static Long parseLong(String value) {
        try {
            return value == null ? null : Long.parseLong(value.trim());
        } catch (NumberFormatException e) {
            return null;
        }
    }

    static String truncate(String value) {
        return value == null || value.length() <= MAX_MESSAGE_LENGTH ? value : value.substring(0, MAX_MESSAGE_LENGTH);
    }
}
