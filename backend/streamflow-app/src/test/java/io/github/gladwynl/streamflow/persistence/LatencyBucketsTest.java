package io.github.gladwynl.streamflow.persistence;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class LatencyBucketsTest {

    @Test
    void bucketsAreUpperInclusiveAndTheLastIsOpenEnded() {
        assertThat(LatencyBuckets.indexOf(-5)).isZero();
        assertThat(LatencyBuckets.indexOf(0)).isZero();
        assertThat(LatencyBuckets.indexOf(1)).isZero();
        assertThat(LatencyBuckets.indexOf(2)).isEqualTo(1);
        assertThat(LatencyBuckets.indexOf(4)).isEqualTo(3);
        assertThat(LatencyBuckets.indexOf(60_000)).isEqualTo(LatencyBuckets.BUCKETS - 2);
        assertThat(LatencyBuckets.indexOf(60_001)).isEqualTo(LatencyBuckets.BUCKETS - 1);
        assertThat(LatencyBuckets.upperBound(0)).isEqualTo(1L);
        assertThat(LatencyBuckets.upperBound(LatencyBuckets.BUCKETS - 1)).isNull();
    }

    @Test
    void quantilesAreTheUpperBoundOfTheContainingBucket() {
        long[] counts = new long[LatencyBuckets.BUCKETS];
        assertThat(LatencyBuckets.quantile(counts, 0.5)).isNull();

        counts[LatencyBuckets.indexOf(4)] = 90; // (3, 5]
        counts[LatencyBuckets.indexOf(120)] = 9; // (100, 150]
        counts[LatencyBuckets.indexOf(2_500)] = 1; // (2000, 3000]
        assertThat(LatencyBuckets.quantile(counts, 0.50)).isEqualTo(5L);
        assertThat(LatencyBuckets.quantile(counts, 0.90)).isEqualTo(5L);
        assertThat(LatencyBuckets.quantile(counts, 0.95)).isEqualTo(150L);
        assertThat(LatencyBuckets.quantile(counts, 0.99)).isEqualTo(150L);
        assertThat(LatencyBuckets.quantile(counts, 1.0)).isEqualTo(3_000L);
    }

    @Test
    void theOpenEndedBucketReportsTheLastFiniteBound() {
        long[] counts = new long[LatencyBuckets.BUCKETS];
        counts[LatencyBuckets.BUCKETS - 1] = 3;
        assertThat(LatencyBuckets.quantile(counts, 0.5)).isEqualTo(60_000L);
    }
}
