package org.apache.flink.runtime.standalone_stackhistogram;
/*
 * QuickMRC - Miss Rate Curve computation from Stack Histograms
 * 
 * Based on Quickmrc design (Section 3.4.2) from the A4S paper.
 * Computes unscaled and scaled miss rate curves from merged stack distance histograms.
 */

import java.util.ArrayList;
import java.util.List;

/**
 * Computes Miss Rate Curves (MRC) from merged StackHistogram objects.
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
public class QuickMRC {

    /**
     * Represents a point on the Miss Rate Curve: cache size -> miss rate.
     */
    public static class MRCPoint {
        private final long cacheSize;
        private final double missRate;

        public MRCPoint(long cacheSize, double missRate) {
            this.cacheSize = cacheSize;
            this.missRate = missRate;
        }

        public long getCacheSize() {
            return cacheSize;
        }

        public double getMissRate() {
            return missRate;
        }

        @Override
        public String toString() {
            return String.format("MRCPoint{cacheSize=%d, missRate=%.6f}", cacheSize, missRate);
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
     *   <li>For each bucket i, compute the maximum stack distance represented by that bucket</li>
     *   <li>For each cache size (incrementing by bucket width), compute cumulative frequency</li>
     *   <li>Miss rate at cache size S = 1 - (cumulative frequency for distances <= S) / total frequency</li>
     * </ol>
     * 
     * @param mergedHistogram The merged stack histogram (result of StackHistogram.merge())
     * @param cacheSizeStep Step size for cache sizes in the MRC (defaults to bucket width if <= 0)
     * @return List of MRCPoint objects representing cache size -> miss rate pairs
     */
    public static List<MRCPoint> computeUnscaledMRC(StackHistogram mergedHistogram, long cacheSizeStep) {
        if (mergedHistogram == null) {
            throw new IllegalArgumentException("Merged histogram cannot be null");
        }

        int numBuckets = mergedHistogram.getNumBuckets();
        long maxStackDistance = mergedHistogram.getMaxStackDistance();
        long totalFrequency = mergedHistogram.getTotalFrequency();

        if (totalFrequency == 0) {
            // Empty histogram - return empty MRC
            return new ArrayList<>();
        }

        // Compute bucket width
        double bucketWidth = (double) maxStackDistance / numBuckets;

        // If cacheSizeStep not specified, use bucket width
        if (cacheSizeStep <= 0) {
            cacheSizeStep = Math.max(1, (long) Math.ceil(bucketWidth));
        }

        // Compute MRC points
        // For each cache size S, we compute miss rate as:
        // miss_rate(S) = 1 - (sum of frequencies for buckets representing distances <= S) / totalFrequency
        List<MRCPoint> mrc = new ArrayList<>();
        long currentCacheSize = 0L;

        while (currentCacheSize <= maxStackDistance) {
            // Count as "hits" only buckets entirely below currentCacheSize.
            // Bucket i covers [i * bucketWidth, (i+1) * bucketWidth). It is entirely below S
            // when (i+1)*bucketWidth <= S, i.e. i < S/bucketWidth, i.e. last hit bucket = floor(S/bucketWidth) - 1.
            // At cache size 0 we have no capacity, so no hits (miss rate 1.0).
            int lastHitBucket = (int) (currentCacheSize / bucketWidth) - 1;

            long cumulativeFreqAtSize = 0L;
            if (lastHitBucket >= 0) {
                for (int i = 0; i <= lastHitBucket && i < numBuckets; i++) {
                    cumulativeFreqAtSize += mergedHistogram.getFrequency(i);
                }
            }

            // Miss rate = 1 - (hits) / total = 1 - cumulativeFreqAtSize / totalFrequency
            double missRate = 1.0 - ((double) cumulativeFreqAtSize / totalFrequency);

            // Ensure miss rate is in valid range [0, 1]
            missRate = Math.max(0.0, Math.min(1.0, missRate));

            mrc.add(new MRCPoint(currentCacheSize, missRate));

            currentCacheSize += cacheSizeStep;
        }

        return mrc;
    }

    /**
     * Computes the unscaled miss rate curve with default step size (bucket width).
     * 
     * @param mergedHistogram The merged stack histogram
     * @return List of MRCPoint objects representing cache size -> miss rate pairs
     */
    public static List<MRCPoint> computeUnscaledMRC(StackHistogram mergedHistogram) {
        return computeUnscaledMRC(mergedHistogram, 0);
    }

    /**
     * Computes the scaled (horizontally scaled) miss rate curve from a merged stack histogram.
     * 
     * <p>Per the paper: scale horizontally by number of tasks. The cache size axis is multiplied
     * by the given factor (e.g. numTasks); miss rate is unchanged.
     * 
     * <p>So each point (cacheSize, missRate) becomes (cacheSize * horizontalScaleFactor, missRate).
     * 
     * @param mergedHistogram The merged stack histogram
     * @param horizontalScaleFactor Factor to multiply cache size by (e.g. numPartitions)
     * @param cacheSizeStep Step size for cache sizes in the unscaled MRC (defaults to bucket width if <= 0)
     * @return List of MRCPoint objects: (scaled cache size, miss rate) pairs
     */
    public static List<MRCPoint> computeScaledMRC(
            StackHistogram mergedHistogram, long horizontalScaleFactor, long cacheSizeStep) {
        List<MRCPoint> unscaledMRC = computeUnscaledMRC(mergedHistogram, cacheSizeStep);
        
        if (horizontalScaleFactor <= 0) {
            horizontalScaleFactor = 1;
        }
        
        List<MRCPoint> scaledMRC = new ArrayList<>();
        for (MRCPoint point : unscaledMRC) {
            long scaledCacheSize = point.getCacheSize() * horizontalScaleFactor;
            scaledMRC.add(new MRCPoint(scaledCacheSize, point.getMissRate()));
        }
        
        return scaledMRC;
    }

    /**
     * Computes the scaled MRC with horizontal scaling by number of tasks (numPartitions).
     * 
     * @param mergedHistogram The merged stack histogram
     * @param cacheSizeStep Step size for cache sizes in the unscaled MRC (defaults to bucket width if <= 0)
     * @return List of MRCPoint objects: (cache size * numPartitions, miss rate) pairs
     */
    public static List<MRCPoint> computeScaledMRC(StackHistogram mergedHistogram, long cacheSizeStep) {
        int numPartitions = mergedHistogram.getNumPartitions();
        long horizontalScaleFactor = numPartitions > 0 ? numPartitions : 1L;
        return computeScaledMRC(mergedHistogram, horizontalScaleFactor, cacheSizeStep);
    }

    /**
     * Computes the scaled MRC with default step size and horizontal scaling by numPartitions.
     * 
     * @param mergedHistogram The merged stack histogram
     * @return List of MRCPoint objects: (cache size * numPartitions, miss rate) pairs
     */
    public static List<MRCPoint> computeScaledMRC(StackHistogram mergedHistogram) {
        return computeScaledMRC(mergedHistogram, 0);
    }
}
