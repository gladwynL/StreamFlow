package io.github.gladwynl.streamflow.events;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;

class EventIdsTest {

    @Test
    void producesVersion7IdsCarryingTheTimestamp() {
        UUID id = new EventIds().next(1_762_800_000_000L);

        assertThat(EventIds.isVersion7(id)).isTrue();
        assertThat(EventIds.timestampMillis(id)).isEqualTo(1_762_800_000_000L);
    }

    @Test
    void idsWithinOneMillisecondAreUniqueAndStrictlyIncreasing() {
        EventIds ids = new EventIds();
        List<String> generated = new ArrayList<>();
        for (int i = 0; i < 10_000; i++) {
            // All in the same millisecond: forces counter increments and overflow into the next millisecond.
            generated.add(ids.next(1_762_800_000_000L).toString());
        }

        assertThat(new HashSet<>(generated)).hasSize(10_000);
        assertThat(generated).isSorted();
        assertThat(generated)
                .allSatisfy(id ->
                        assertThat(EventIds.isVersion7(UUID.fromString(id))).isTrue());
    }

    @Test
    void aClockGoingBackwardsDoesNotBreakOrdering() {
        EventIds ids = new EventIds();
        UUID later = ids.next(2_000_000_000_000L);
        UUID afterClockStep = ids.next(1_000_000_000_000L);

        assertThat(afterClockStep.toString()).isGreaterThan(later.toString());
    }
}
