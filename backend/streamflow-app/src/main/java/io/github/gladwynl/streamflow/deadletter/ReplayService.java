package io.github.gladwynl.streamflow.deadletter;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.gladwynl.streamflow.events.EventHeaders;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.DeadLetter;
import io.github.gladwynl.streamflow.processing.CrashInjector;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/**
 * Replays dead letters to their source topic with their original key, value and event identity.
 *
 * <p>Per dead letter: (1) claim it ({@code PENDING → REPLAY_REQUESTED}, conditional, so concurrent requests cannot
 * both win); (2) publish the original record and wait for the broker acknowledgement; (3) mark it {@code REPLAYED}.
 * If publication definitely fails, the claim is released. If the replayer crashes between (2) and (3), the claim
 * stays until the sweeper releases it; replaying it again publishes the same event id a second time, which the
 * processing ledger suppresses. Replay therefore never applies an event twice.
 *
 * <p>Replayed events are processed after events that were published while they sat in the dead-letter table:
 * per-key order is not preserved across a dead-letter and replay (ADR-0007).
 */
public final class ReplayService {

    private static final Logger log = LoggerFactory.getLogger(ReplayService.class);
    private static final Duration SEND_TIMEOUT = Duration.ofSeconds(15);
    private static final List<String> IDENTITY_HEADERS =
            List.of(EventHeaders.RUN_ID, EventHeaders.EVENT_ID, EventHeaders.SEQUENCE, EventHeaders.SCHEMA_VERSION);

    public enum Outcome {
        /** Published and marked REPLAYED. */
        REPLAYED,
        /** No such dead letter. */
        NOT_FOUND,
        /** Not PENDING: already replayed, discarded, expired, or claimed by another replay. */
        NOT_PENDING,
        /** Dead-lettered before the replay window; replaying could defeat deduplication. */
        OUTSIDE_REPLAY_WINDOW,
        /** The record carries no event id, so replaying it could not be deduplicated. */
        NO_EVENT_ID,
        /** Publication failed; the dead letter is PENDING again. */
        PUBLISH_FAILED
    }

    public record Result(long id, Outcome outcome, String detail) {}

    private final DeadLetterRepository repository;
    private final Producer<String, byte[]> producer;
    private final Duration replayWindow;
    private final CrashInjector crashInjector;
    private final JsonMapper json = JsonMapper.builder().build();

    public ReplayService(
            DeadLetterRepository repository,
            Producer<String, byte[]> producer,
            Duration replayWindow,
            CrashInjector crashInjector) {
        this.repository = repository;
        this.producer = producer;
        this.replayWindow = replayWindow;
        this.crashInjector = crashInjector;
    }

    public List<Result> replay(List<Long> ids) {
        List<Long> distinct = ids.stream().distinct().toList();
        UUID claimToken = UUID.randomUUID();
        Map<Long, DeadLetter> claimed = new LinkedHashMap<>();
        repository.claim(distinct, claimToken, replayWindow).forEach(d -> claimed.put(d.id(), d));

        List<Result> results = new ArrayList<>();
        for (long id : distinct) {
            DeadLetter deadLetter = claimed.get(id);
            results.add(deadLetter == null ? rejection(id) : publish(deadLetter, claimToken));
        }
        return results;
    }

    private Result publish(DeadLetter deadLetter, UUID claimToken) {
        ProducerRecord<String, byte[]> record = new ProducerRecord<>(
                deadLetter.sourceTopic(), null, deadLetter.recordKey(), deadLetter.payload(), headers(deadLetter));
        try {
            producer.send(record).get(SEND_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            // Outcome unknown: keep the claim; the sweeper releases it and the ledger absorbs a second publication.
            return new Result(deadLetter.id(), Outcome.PUBLISH_FAILED, "interrupted; claim kept for the sweeper");
        } catch (ExecutionException | TimeoutException e) {
            // Possibly published (timeout); releasing is still safe because the ledger suppresses a duplicate.
            repository.release(deadLetter.id(), claimToken);
            log.warn("Replay of dead letter {} failed: {}", deadLetter.id(), e.toString());
            return new Result(deadLetter.id(), Outcome.PUBLISH_FAILED, e.toString());
        }
        crashInjector.afterReplayPublished(deadLetter.id(), deadLetter.eventId());
        repository.markReplayed(deadLetter.id(), claimToken);
        log.info("Replayed dead letter {} (event {})", deadLetter.id(), deadLetter.eventId());
        return new Result(deadLetter.id(), Outcome.REPLAYED, null);
    }

    private Result rejection(long id) {
        return repository
                .find(id)
                .map(d -> {
                    if (d.status() != DeadLetterRepository.Status.PENDING) {
                        return new Result(id, Outcome.NOT_PENDING, "status is " + d.status());
                    }
                    if (d.eventId() == null) {
                        return new Result(id, Outcome.NO_EVENT_ID, "record has no event id; discard it instead");
                    }
                    return new Result(id, Outcome.OUTSIDE_REPLAY_WINDOW, "dead-lettered at " + d.deadLetteredAt());
                })
                .orElse(new Result(id, Outcome.NOT_FOUND, null));
    }

    /** The original identity headers, plus a link to the dead letter being replayed. */
    private RecordHeaders headers(DeadLetter deadLetter) {
        RecordHeaders headers = new RecordHeaders();
        JsonNode original = json.readTree(deadLetter.headersJson());
        for (String name : IDENTITY_HEADERS) {
            JsonNode value = original.get(name);
            if (value != null && !value.isNull()) {
                headers.add(name, value.asString().getBytes(UTF_8));
            }
        }
        headers.add(DeadLetterHeaders.REPLAY_OF, Long.toString(deadLetter.id()).getBytes(UTF_8));
        return headers;
    }
}
