package io.github.gladwynl.streamflow.deadletter;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

/** Header values must never reach PostgreSQL text or jsonb with bytes it cannot store (NUL, invalid UTF-8). */
class DeadLetterIngestionTest {

    @Test
    void printableUtf8IsKeptAsText() {
        assertThat(DeadLetterIngestion.printable("Invalid event: userId has an invalid format\n".getBytes(UTF_8)))
                .isEqualTo("Invalid event: userId has an invalid format\n");
        assertThat(DeadLetterIngestion.printable("café ✓".getBytes(UTF_8))).isEqualTo("café ✓");
    }

    @Test
    void binaryValuesAreRejectedSoTheyAreStoredAsBase64() {
        // Spring's kafka_deliveryAttempt header: a 4-byte big-endian int, mostly NUL bytes.
        assertThat(DeadLetterIngestion.printable(
                        ByteBuffer.allocate(4).putInt(1).array()))
                .isNull();
        assertThat(DeadLetterIngestion.printable(new byte[] {(byte) 0xC3, (byte) 0x28}))
                .as("invalid UTF-8")
                .isNull();
    }

    @Test
    void longMessagesAreTruncated() {
        assertThat(DeadLetterIngestion.truncate("x".repeat(5_000))).hasSize(DeadLetterIngestion.MAX_MESSAGE_LENGTH);
        assertThat(DeadLetterIngestion.truncate(null)).isNull();
    }
}
