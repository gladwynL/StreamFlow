package io.github.gladwynl.streamflow.api.simulations;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.gladwynl.streamflow.api.simulations.SimulationController.StartRequest;
import io.github.gladwynl.streamflow.events.EventType;
import io.github.gladwynl.streamflow.simulation.SimulationSpec;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.web.server.ResponseStatusException;

/** Simulation bounds: at most 500,000 events, 5,000 events/s, 600 s and 100,000 users per run. */
class SimulationControllerTest {

    private final SimulationController controller = new SimulationController(null, null, null, null, null, null);

    @Test
    void acceptsARunWithinTheBounds() {
        SimulationSpec spec = controller.validate(
                new StartRequest(42L, 500_000L, null, 5_000, 100_000, List.of("PURCHASE_COMPLETED", "USER_LOGIN")));
        assertThat(spec.seed()).isEqualTo(42);
        assertThat(spec.events()).isEqualTo(500_000);
        assertThat(spec.eventTypes()).containsExactlyInAnyOrder(EventType.PURCHASE_COMPLETED, EventType.USER_LOGIN);
    }

    @Test
    void aDurationSetsTheEventCountAndASeedIsChosenWhenMissing() {
        SimulationSpec spec = controller.validate(new StartRequest(null, null, 30, 200, null, null));
        assertThat(spec.events()).isEqualTo(6_000);
        assertThat(spec.seed()).isNotNegative();
        assertThat(spec.users()).isEqualTo(SimulationController.DEFAULT_USERS);
        assertThat(spec.eventTypes()).hasSize(EventType.values().length);
    }

    @Test
    void rejectsRunsOutsideTheBounds() {
        rejected(null, "request body");
        rejected(new StartRequest(1L, 500_001L, null, 5_000, null, null), "events");
        rejected(new StartRequest(1L, 1_000L, null, 5_001, null, null), "ratePerSecond");
        rejected(new StartRequest(1L, 1_000L, null, 0, null, null), "ratePerSecond");
        rejected(new StartRequest(1L, 6_001L, null, 10, null, null), "longer than 600 s");
        rejected(new StartRequest(1L, null, 601, 10, null, null), "durationSeconds");
        rejected(new StartRequest(1L, 10L, 10, 10, null, null), "either events or durationSeconds");
        rejected(new StartRequest(1L, null, null, 10, null, null), "required");
        rejected(new StartRequest(1L, 10L, null, 10, 100_001, null), "users");
        rejected(new StartRequest(1L, 10L, null, 10, null, List.of("NOT_A_TYPE")), "Unknown event type");
        rejected(new StartRequest(1L, 10L, null, 10, null, List.of()), "must not be empty");
    }

    private void rejected(StartRequest request, String message) {
        assertThatThrownBy(() -> controller.validate(request))
                .isInstanceOfSatisfying(
                        ResponseStatusException.class,
                        e -> assertThat(e.getStatusCode()).isEqualTo(HttpStatus.BAD_REQUEST))
                .hasMessageContaining(message);
    }
}
