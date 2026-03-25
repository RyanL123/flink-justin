package org.apache.flink.runtime.a4s.core;
/*
 * MissRateCurve - Miss Rate Curve computation from Stack Histograms
 *
 * Based on Quickmrc design (Section 3.4.2) from the A4S paper.
 * Computes unscaled and scaled miss rate curves from merged stack distance histograms.
 */

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Computes Miss Rate Curves (MRC) from merged StackDistanceHistogram objects.
 *
 * <p>The MRC shows the miss rate as a function of cache size. Given a merged stack distance
 * histogram, we can compute:
 * <ul>
 *   <li><b>Unscaled MRC</b>: Raw miss rate curve (cache size vs miss rate)</li>
 *   <li><b>Scaled MRC</b>: Horizontally scaled curve: cache size is scaled by number of tasks
 *       (per the paper); miss rate is unchanged.</li>
 * </ul>
 *
 * <p>For a given cache size S:
 * <ul>
 *   <li>Miss rate = (sum of frequencies for stack distances > S) / total accesses</li>
 *   <li>Or equivalently: miss rate = 1 - (cumulative frequency for stack distances <= S) / total accesses</li>
 * </ul>
 */
public class MissRateCurve {
    private static final long CACHE_ITEM_SIZE_BYTES = 4096L;

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
            return String.format("Point{cacheSizeBytes=%d, missRate=%.6f}", cacheSizeBytes, missRate);
        }
    }

    /**
     * Computes the unscaled miss rate curve from a merged stack histogram.
     *
     * <p>The unscaled MRC shows the raw miss rate for each cache size. For a cache size S,
     * the miss rate is computed as the fraction of accesses with stack distance > S.
     *
     * <p>Algorithm:
     * <ol>
     *   <li>Bucket position (1-based) is treated as the cache-size unit</li>
     *   <li>For each bucket position, compute cumulative frequency</li>
     *   <li>Miss rate at size S = 1 - (cumulative frequency for positions <= S) / total frequency</li>
     * </ol>
     *
     * @param mergedHistogram The merged stack histogram (result of StackDistanceHistogram.merge())
     * @param cacheItemSizeBytes cache item size in bytes
     * @param bucketSizeScaling bucket size scaling
     * @return List of Point objects representing cache size -> miss rate pairs
     */
    public static List<Point> computeUnscaledMRC(
            StackDistanceHistogram mergedHistogram, long cacheItemSizeBytes, long bucketSizeScaling) {
        if (mergedHistogram == null) {
            throw new IllegalArgumentException("Merged histogram cannot be null");
        }

        if (cacheItemSizeBytes <= 0) {
            cacheItemSizeBytes = CACHE_ITEM_SIZE_BYTES;
        }

        long totalFrequency = mergedHistogram.getTotalFrequency();
        if (totalFrequency == 0) {
            return new ArrayList<>();
        }

        // last bucket is used for tracking complete misses
        int numBuckets = mergedHistogram.getNumBuckets() - 1;

        List<Point> mrc = new ArrayList<>();
        long cumulativeFreqAtSize = 0L;
        for (int i = 0; i < numBuckets; i++) {
            cumulativeFreqAtSize += mergedHistogram.getFrequency(i);
            long currentCacheSize = (i + 1L) * bucketSizeScaling;
            double missRate = 1.0 - ((double) cumulativeFreqAtSize / totalFrequency);
            missRate = Math.max(0.0, Math.min(1.0, missRate));
            mrc.add(new Point(currentCacheSize * cacheItemSizeBytes, missRate));
        }

        return mrc;
    }

    /**
     * Computes the scaled (horizontally scaled) miss rate curve from a merged stack histogram.
     *
     * <p>Per the paper: scale horizontally by number of tasks. The cache size axis is multiplied
     * by the given factor (e.g. numTasks); miss rate is unchanged.
     *
     * <p>The input histogram axis is interpreted as bucket units. To convert this into memory
     * capacity for the generated MRC, callers provide the bucket size scaling in bytes.
     * Output MRC points use bytes on the x-axis.
     *
     * @param mergedHistogram The merged stack histogram
     * @param cacheItemSizeBytes cache item size in bytes
     * @param bucketSizeScaling bucket size scaling
     * @return List of Point objects: (scaled cache size in bytes, miss rate) pairs
     */
    public static List<Point> computeScaledMRC(
            StackDistanceHistogram mergedHistogram, long cacheItemSizeBytes, long bucketSizeScaling) {
        List<Point> unscaledMRC =
                computeUnscaledMRC(mergedHistogram, cacheItemSizeBytes, bucketSizeScaling);

        List<Point> scaledMRC = new ArrayList<>();
        for (Point point : unscaledMRC) {
            long cacheSizeItems = point.getCacheSizeBytes();
            long scaledCacheSizeBytes = cacheSizeItems * mergedHistogram.getNumPartitions();
            scaledMRC.add(new Point(scaledCacheSizeBytes, point.getMissRate()));
        }

        return scaledMRC;
    }
}
