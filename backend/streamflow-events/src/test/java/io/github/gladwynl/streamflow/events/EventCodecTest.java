package io.github.gladwynl.streamflow.events;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class EventCodecTest {

    private final EventCodec codec = new EventCodec();

    @Test
    void roundTripsEveryField() {
        EventEnvelope event = TestEvents.purchase();

        assertThat(codec.decode(codec.encode(event))).isEqualTo(event);
    }

    @Test
    void writesInstantsAsIsoStringsAndTheEnvelopeFieldsByName() {
        String json = new String(codec.encode(TestEvents.purchase()), UTF_8);

        assertThat(json)
                .contains("\"schemaVersion\":1")
                .contains("\"eventType\":\"PURCHASE_COMPLETED\"")
                .contains("\"occurredAt\":\"2026-11-10T19:00:00.123456Z\"")
                .contains("\"amountCents\":4599");
    }

    @Test
    void rejectsUnsupportedSchemaVersion() {
        assertRejected(json().replace("\"schemaVersion\":1", "\"schemaVersion\":2"), "Unsupported schemaVersion: 2");
    }

    @Test
    void rejectsUnknownEventType() {
        assertRejected(json().replace("PURCHASE_COMPLETED", "REFUND_ISSUED"), "Unknown eventType: REFUND_ISSUED");
    }

    @Test
    void rejectsUnknownEnvelopeField() {
        assertRejected(json().replace("{\"schemaVersion\"", "{\"extra\":1,\"schemaVersion\""), "Malformed event JSON");
    }

    @Test
    void rejectsPayloadOfAnotherType() {
        String loginPayload =
                json().replaceFirst("\"payload\":\\{.*}}", "\"payload\":{\"method\":\"SSO\",\"success\":true}}");

        assertRejected(loginPayload, "Malformed PURCHASE_COMPLETED payload");
    }

    @Test
    void rejectsTrailingContentAndEmptyInput() {
        assertRejected(json() + "{}", "Malformed event JSON");
        assertThatThrownBy(() -> codec.decode(new byte[0])).isInstanceOf(EventDecodingException.class);
        assertRejected("null", "Event is JSON null");
        assertRejected("not json", "Malformed event JSON");
    }

    @Test
    void decodesMissingFieldsAsNullForTheValidatorToReport() {
        EventEnvelope decoded =
                codec.decode(json().replace("\"userId\":\"u-004211\",", "").getBytes(UTF_8));

        assertThat(decoded.userId()).isNull();
        assertThat(new EventValidator().violations(decoded)).containsExactly("userId is required");
    }

    private String json() {
        return new String(codec.encode(TestEvents.purchase()), UTF_8);
    }

    private void assertRejected(String json, String message) {
        assertThatThrownBy(() -> codec.decode(json.getBytes(UTF_8)))
                .isInstanceOf(EventDecodingException.class)
                .hasMessageContaining(message);
    }
}
