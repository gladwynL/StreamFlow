package io.github.gladwynl.streamflow.api.dashboard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.gladwynl.streamflow.persistence.DashboardQueries.MinutePoint;
import io.github.gladwynl.streamflow.persistence.LatencyBuckets;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.web.server.ResponseStatusException;

class DashboardControllerTest {

    @Test
    void cursorsRoundTripAndRejectGarbage() {
        DashboardController.Cursor cursor =
                new DashboardController.Cursor(Instant.parse("2026-11-11T10:15:30.123456Z"), UUID.randomUUID());
        assertThat(DashboardController.Cursor.decode(cursor.encode())).isEqualTo(cursor);
        assertThatThrownBy(() -> DashboardController.Cursor.decode("not-a-cursor"))
                .isInstanceOf(ResponseStatusException.class);
        assertThatThrownBy(() -> DashboardController.Range.parse("7d")).isInstanceOf(ResponseStatusException.class);
    }

    @Test
    void seriesPointsDeriveRatesAndPercentilesFromTheMinute() {
        long[] buckets = new long[LatencyBuckets.BUCKETS];
        buckets[LatencyBuckets.indexOf(8)] = 100;
        DashboardController.SeriesPoint point =
                DashboardController.point(new MinutePoint(Instant.EPOCH, 120, 5_000, 2, buckets));
        assertThat(point.processedPerSecond()).isEqualTo(2.0);
        assertThat(point.latencySamples()).isEqualTo(100);
        assertThat(point.p50Millis()).isEqualTo(10L);

        DashboardController.SeriesPoint empty =
                DashboardController.point(new MinutePoint(Instant.EPOCH, 0, 0, 3, new long[LatencyBuckets.BUCKETS]));
        assertThat(empty.p50Millis()).isNull();
        assertThat(empty.latencySamples()).isZero();
    }
}
