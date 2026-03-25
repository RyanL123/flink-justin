package org.apache.flink.runtime.a4s.core;
/*
 * MissRateCurve - Miss Rate Curve computation from Stack Histograms
 *
 * Based on Quickmrc design (Section 3.4.2) from the A4S paper.
 * Computes unscaled and scaled miss rate curves from merged stack distance histograms.
 */

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Computes Miss Rate Curves (MRC) from merged StackDistanceHistogram objects.
 *
 * <p>The MRC shows the miss rate as a function of cache size. Given a merged stack distance
 * histogram, we can compute:
 *
 * <ul>
 *   <li><b>Unscaled MRC</b>: Raw miss rate curve (cache size vs miss rate)
 *   <li><b>Scaled MRC</b>: Horizontally scaled curve: cache size is scaled by number of tasks (per
 *       the paper); miss rate is unchanged.
 * </ul>
 *
 * <p>For a given cache size S:
 *
 * <ul>
 *   <li>Miss rate = (sum of frequencies for stack distances > S) / total accesses
 *   <li>Or equivalently: miss rate = 1 - (cumulative frequency for stack distances <= S) / total
 *       accesses
 * </ul>
 */
public class MissRateCurve {
    private static final long CACHE_ITEM_SIZE_BYTES = 4096L;
    private final List<Point> points;

    @JsonCreator
    public MissRateCurve(List<Point> points) {
        this.points = points == null ? Collections.emptyList() : List.copyOf(points);
    }

    @JsonValue
    public List<Point> getPoints() {
        return points;
    }

    /** Represents a point on the Miss Rate Curve: cache size -> miss rate. */
    public static class Point {
        public static final String FIELD_NAME_CACHE_SIZE_BYTES = "cacheSizeBytes";
        public static final String FIELD_NAME_MISS_RATE = "missRate";

        @JsonProperty(FIELD_NAME_CACHE_SIZE_BYTES)
        private final long cacheSizeBytes;

        @JsonProperty(FIELD_NAME_MISS_RATE)
        private final double missRate;

        @JsonCreator
        public Point(
                @JsonProperty(FIELD_NAME_CACHE_SIZE_BYTES) long cacheSizeBytes,
                @JsonProperty(FIELD_NAME_MISS_RATE) double missRate) {
            this.cacheSizeBytes = cacheSizeBytes;
            this.missRate = missRate;
        }

        public long getCacheSizeBytes() {
            return cacheSizeBytes;
        }

        public double getMissRate() {
            return missRate;
        }

        @Override
        public String toString() {
            return String.format(
                    "Point{cacheSizeBytes=%d, missRate=%.6f}", cacheSizeBytes, missRate);
        }
    }

    public static MissRateCurve fromStackDistanceHistogram(
            StackDistanceHistogram mergedHistogram,
            long cacheItemSizeBytes,
            long bucketSizeScaling) {
        if (mergedHistogram == null) {
            throw new IllegalArgumentException("Merged histogram cannot be null");
        }

        if (cacheItemSizeBytes <= 0) {
            cacheItemSizeBytes = CACHE_ITEM_SIZE_BYTES;
        }

        long totalFrequency = mergedHistogram.getTotalFrequency();
        if (totalFrequency == 0) {
            return new MissRateCurve(Collections.emptyList());
        }

        // last bucket is used for tracking complete misses
        int numBuckets = mergedHistogram.getNumBuckets() - 1;

        List<Point> scaledMrc = new ArrayList<>();
        long cumulativeFreqAtSize = 0L;
        for (int i = 0; i < numBuckets; i++) {
            cumulativeFreqAtSize += mergedHistogram.getFrequency(i);
            long currentCacheSize = (i + 1L) * bucketSizeScaling;
            double missRate = 1.0 - ((double) cumulativeFreqAtSize / totalFrequency);
            missRate = Math.max(0.0, Math.min(1.0, missRate));
            long cacheSizeItems = currentCacheSize * cacheItemSizeBytes;
            long scaledCacheSizeBytes = cacheSizeItems * mergedHistogram.getNumPartitions();
            scaledMrc.add(new Point(scaledCacheSizeBytes, missRate));
        }

        return new MissRateCurve(scaledMrc);
    }

    @Override
    public String toString() {
        if (points.isEmpty()) {
            return "MissRateCurve{points=[]}";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("MissRateCurve{points=[");
        for (int i = 0; i < points.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Point p = points.get(i);
            double cacheMiB = p.getCacheSizeBytes() / (1024.0 * 1024.0);
            sb.append(String.format("cacheSizeMiB=%.6f missRate=%.6f", cacheMiB, p.getMissRate()));
        }
        sb.append("]}");
        return sb.toString();
    }
}
