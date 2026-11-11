package io.github.gladwynl.streamflow.api.deadletters;

import static java.nio.charset.StandardCharsets.UTF_8;

import io.github.gladwynl.streamflow.api.security.OperatorGuard;
import io.github.gladwynl.streamflow.config.StreamflowProperties;
import io.github.gladwynl.streamflow.deadletter.ReplayService;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.DeadLetter;
import io.github.gladwynl.streamflow.persistence.DeadLetterRepository.Status;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.time.Instant;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.springframework.context.annotation.Profile;
import org.springframework.http.CacheControl;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

/**
 * Dead-letter inspection (read-only) and operator actions (replay, discard). See docs/api.md.
 */
@RestController
@RequestMapping("/api/v1/dead-letters")
@Profile("api")
class DeadLetterController {

    private static final int MAX_PAGE = 100;
    private static final int PAYLOAD_PREVIEW = 4_096;
    private static final int MAX_REASON = 200;

    private final DeadLetterRepository deadLetters;
    private final ReplayService replay;
    private final OperatorGuard guard;
    private final int maxReplayBatch;
    private final JsonMapper json = JsonMapper.builder().build();

    DeadLetterController(
            DeadLetterRepository deadLetters,
            ReplayService replay,
            OperatorGuard guard,
            StreamflowProperties properties) {
        this.deadLetters = deadLetters;
        this.replay = replay;
        this.guard = guard;
        this.maxReplayBatch = properties.deadLetters().maxReplayBatch();
    }

    @GetMapping
    ResponseEntity<List<Summary>> list(
            @RequestParam(required = false) String status,
            @RequestParam(required = false) Long before,
            @RequestParam(defaultValue = "50") int limit) {
        if (limit < 1 || limit > MAX_PAGE) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "limit must be between 1 and " + MAX_PAGE);
        }
        List<Summary> page = deadLetters.list(parseStatus(status), before, limit).stream()
                .map(Summary::of)
                .toList();
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(page);
    }

    @GetMapping("/{id}")
    ResponseEntity<Detail> get(@PathVariable long id) {
        DeadLetter d = deadLetters
                .find(id)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "No dead letter " + id));
        Map<String, Object> headers = json.readValue(d.headersJson(), new TypeReference<>() {});
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(Detail.of(d, headers));
    }

    @PostMapping("/replay")
    List<ReplayService.Result> replay(
            @RequestHeader(name = OperatorGuard.HEADER, required = false) String key,
            @RequestBody ReplayRequest request) {
        guard.authorize(key);
        if (request == null || request.ids() == null || request.ids().isEmpty()) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ids must not be empty");
        }
        if (request.ids().size() > maxReplayBatch) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "at most " + maxReplayBatch + " ids per request");
        }
        if (request.ids().stream().anyMatch(id -> id == null || id < 1)) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "ids must be positive");
        }
        return replay.replay(request.ids());
    }

    @PostMapping("/{id}/discard")
    Map<String, Object> discard(
            @RequestHeader(name = OperatorGuard.HEADER, required = false) String key,
            @PathVariable long id,
            @RequestBody DiscardRequest request) {
        guard.authorize(key);
        String reason = request == null || request.reason() == null
                ? ""
                : request.reason().strip();
        if (reason.isEmpty() || reason.length() > MAX_REASON) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST, "reason is required (at most " + MAX_REASON + " characters)");
        }
        if (deadLetters.find(id).isEmpty()) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "No dead letter " + id);
        }
        if (!deadLetters.discard(id, reason)) {
            throw new ResponseStatusException(
                    HttpStatus.CONFLICT, "Only PENDING or EXPIRED dead letters can be discarded");
        }
        return Map.of("id", id, "status", Status.DISCARDED.name());
    }

    private static Status parseStatus(String status) {
        if (status == null || status.isBlank()) {
            return null;
        }
        try {
            return Status.valueOf(status);
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "Unknown status " + status);
        }
    }

    record ReplayRequest(List<Long> ids) {}

    record DiscardRequest(String reason) {}

    record Summary(
            long id,
            Status status,
            String failureCategory,
            String errorMessage,
            UUID eventId,
            UUID runId,
            Long runSeq,
            String sourceTopic,
            int sourcePartition,
            long sourceOffset,
            Instant deadLetteredAt,
            int replayCount) {

        static Summary of(DeadLetter d) {
            return new Summary(
                    d.id(),
                    d.status(),
                    d.failureCategory(),
                    d.errorMessage(),
                    d.eventId(),
                    d.runId(),
                    d.runSeq(),
                    d.sourceTopic(),
                    d.sourcePartition(),
                    d.sourceOffset(),
                    d.deadLetteredAt(),
                    d.replayCount());
        }
    }

    /**
     * @param payloadText the payload decoded as UTF-8 (first 4 KiB), or null if it is not valid UTF-8
     * @param payloadBase64 the payload as Base64 (first 4 KiB) when it is not valid UTF-8
     */
    record Detail(
            Summary summary,
            String exceptionClass,
            Integer attempts,
            String recordKey,
            int payloadBytes,
            String payloadText,
            String payloadBase64,
            Map<String, Object> headers,
            Instant statusChangedAt,
            Instant replayedAt,
            String discardReason) {

        static Detail of(DeadLetter d, Map<String, Object> headers) {
            byte[] payload = d.payload() == null ? new byte[0] : d.payload();
            byte[] preview =
                    payload.length <= PAYLOAD_PREVIEW ? payload : java.util.Arrays.copyOf(payload, PAYLOAD_PREVIEW);
            String text = utf8(preview);
            return new Detail(
                    Summary.of(d),
                    d.exceptionClass(),
                    d.attempts(),
                    d.recordKey(),
                    payload.length,
                    text,
                    text == null ? Base64.getEncoder().encodeToString(preview) : null,
                    headers,
                    d.statusChangedAt(),
                    d.replayedAt(),
                    d.discardReason());
        }

        private static String utf8(byte[] bytes) {
            try {
                return UTF_8.newDecoder()
                        .onMalformedInput(CodingErrorAction.REPORT)
                        .onUnmappableCharacter(CodingErrorAction.REPORT)
                        .decode(ByteBuffer.wrap(bytes))
                        .toString();
            } catch (CharacterCodingException e) {
                return null;
            }
        }
    }
}
