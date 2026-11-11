package io.github.gladwynl.streamflow.persistence;

/**
 * Fixed, log-spaced latency buckets (milliseconds). Bucket {@code i} counts latencies in
 * {@code (UPPER_BOUNDS[i-1], UPPER_BOUNDS[i]]}; the last bucket is open-ended. Percentiles computed from the
 * histogram are reported as the upper bound of the bucket that contains them, so they are exact to the bucket
 * resolution and never understate latency.
 */
public final class LatencyBuckets {

    /** Upper bounds in milliseconds; the final bucket (index {@code UPPER_BOUNDS.length}) has no upper bound. */
    static final long[] UPPER_BOUNDS = {
        1, 2, 3, 5, 7, 10, 15, 20, 30, 50, 70, 100, 150, 200, 300, 500, 700, 1_000, 1_500, 2_000, 3_000, 5_000, 7_000,
        10_000, 15_000, 30_000, 60_000
    };

    public static final int BUCKETS = UPPER_BOUNDS.length + 1;

    private LatencyBuckets() {}

    public static int indexOf(long latencyMillis) {
        long value = Math.max(0, latencyMillis);
        for (int i = 0; i < UPPER_BOUNDS.length; i++) {
            if (value <= UPPER_BOUNDS[i]) {
                return i;
            }
        }
        return UPPER_BOUNDS.length;
    }

    /** Upper bound of bucket {@code index} in milliseconds, or {@code null} for the open-ended last bucket. */
    public static Long upperBound(int index) {
        return index < UPPER_BOUNDS.length ? UPPER_BOUNDS[index] : null;
    }

    /**
     * The upper bound of the bucket containing quantile {@code q} (0 < q ≤ 1) of {@code counts}, or {@code null}
     * when there are no samples. For the open-ended bucket, returns the last finite bound (reported as "&gt;").
     */
    public static Long quantile(long[] counts, double q) {
        long total = 0;
        for (long count : counts) {
            total += count;
        }
        if (total == 0) {
            return null;
        }
        long rank = (long) Math.ceil(q * total);
        long seen = 0;
        for (int i = 0; i < counts.length; i++) {
            seen += counts[i];
            if (seen >= rank) {
                Long bound = upperBound(i);
                return bound != null ? bound : UPPER_BOUNDS[UPPER_BOUNDS.length - 1];
            }
        }
        return UPPER_BOUNDS[UPPER_BOUNDS.length - 1];
    }
}
