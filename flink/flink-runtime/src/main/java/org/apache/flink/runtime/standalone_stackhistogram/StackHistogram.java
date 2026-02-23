package org.apache.flink.runtime.standalone_stackhistogram;
/*
 * Standalone Stack Histogram Module
 * 
 * Based on Quickmrc design (Section 3.4.2) from the A4S paper.
 * Implements stack distance histogram merging logic.
 */

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Represents a stack distance histogram. A stack distance histogram is a frequency distribution
 * where the stack distance range [0, Dmax] is split into H fixed-size histogram buckets.
 * Each bucket records the frequency of stack distances falling in that bucket range.
 * 
 * <p>Based on Quickmrc design (Section 3.4.2):
 * <ul>
 *   <li>Stack distances are calculated using bucket-based LRU approximation</li>
 *   <li>The stack distance range [0, Dmax] is split into H fixed-size histogram buckets</li>
 *   <li>Each bucket records the frequency of stack distances falling in that bucket range</li>
 *   <li>Default H=50 buckets as per Quickmrc implementation</li>
 * </ul>
 * 
 * <p>This class can represent both:
 * <ul>
 *   <li>Individual histograms from a single task/partition</li>
 *   <li>Merged histograms from multiple tasks/partitions</li>
 * </ul>
 */
public class StackHistogram {

    /** Default number of histogram buckets (H) as per Quickmrc implementation. */
    public static final int DEFAULT_NUM_BUCKETS = 50;

    /**
     * Map from bucket index (0 to H-1) to frequency count. The bucket index represents a range of
     * stack distances. For H buckets and max stack distance Dmax, bucket i covers stack distances
     * [i * (Dmax/H), (i+1) * (Dmax/H)).
     */
    private final Map<Integer, Long> histogram;

    /** Number of histogram buckets (H). Default is 50 as per Quickmrc implementation. */
    private final int numBuckets;

    /** Maximum stack distance (Dmax) - total number of keys in LRU and ghost caches. */
    private final long maxStackDistance;

    /**
     * Number of partitions/tasks that contributed to this histogram. For individual histograms,
     * this is 1. For merged histograms, this is the number of partitions that were merged.
     */
    private final int numPartitions;

    /**
     * Creates a new stack histogram.
     *
     * @param histogram Map from bucket index (0 to H-1) to frequency count
     * @param numBuckets Number of histogram buckets H (default 50)
     * @param maxStackDistance Maximum stack distance Dmax
     * @param numPartitions Number of partitions/tasks that contributed (1 for individual, >1 for merged)
     */
    public StackHistogram(
            Map<Integer, Long> histogram,
            int numBuckets,
            long maxStackDistance,
            int numPartitions) {
        this.histogram = new HashMap<>(histogram);
        this.numBuckets = numBuckets;
        this.maxStackDistance = maxStackDistance;
        this.numPartitions = numPartitions;
    }

    /**
     * Creates a new stack histogram with default number of buckets (50) and single partition.
     *
     * @param histogram Map from bucket index to frequency count
     * @param maxStackDistance Maximum stack distance Dmax
     */
    public StackHistogram(Map<Integer, Long> histogram, long maxStackDistance) {
        this(histogram, DEFAULT_NUM_BUCKETS, maxStackDistance, 1);
    }

    /**
     * Creates a {@link StackHistogram} from the JSON payload produced by
     * RocksDBStackDistanceHistogramView.getValue():
     *
     * <pre>
     * {"boundaries":[...],"counts":[...]}
     * </pre>
     *
     * <p>The resulting histogram stores one entry per count index. The overflow bucket (the last
     * element in {@code counts}) is preserved as the last histogram bucket.
     *
     * @param serializedHistogram serialized JSON string from RocksDBStackDistanceHistogramView
     * @return parsed {@link StackHistogram}
     * @throws IllegalArgumentException if the payload is malformed
     */
    public static StackHistogram fromSerializedValue(String serializedHistogram) {
        if (serializedHistogram == null || serializedHistogram.trim().isEmpty()) {
            throw new IllegalArgumentException("Serialized histogram must not be null or empty");
        }

        long[] boundaries = parseLongArrayField(serializedHistogram, "boundaries");
        long[] counts = parseLongArrayField(serializedHistogram, "counts");

        if (counts.length != boundaries.length + 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid histogram payload: counts length (%d) must equal boundaries length + 1 (%d)",
                            counts.length, boundaries.length + 1));
        }

        long maxStackDistance = boundaries.length == 0 ? 0L : boundaries[boundaries.length - 1];
        Map<Integer, Long> histogram = new HashMap<>();
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] < 0L) {
                throw new IllegalArgumentException(
                        "Invalid histogram payload: counts must be non-negative");
            }
            if (counts[i] != 0L) {
                histogram.put(i, counts[i]);
            }
        }

        return new StackHistogram(histogram, counts.length, maxStackDistance, 1);
    }

    private static long[] parseLongArrayField(String json, String fieldName) {
        String key = "\"" + fieldName + "\":[";
        int start = json.indexOf(key);
        if (start < 0) {
            throw new IllegalArgumentException(
                    "Invalid histogram payload: missing field '" + fieldName + "'");
        }

        int arrayStart = start + key.length();
        int arrayEnd = json.indexOf(']', arrayStart);
        if (arrayEnd < 0) {
            throw new IllegalArgumentException(
                    "Invalid histogram payload: unterminated array for field '" + fieldName + "'");
        }

        String body = json.substring(arrayStart, arrayEnd).trim();
        if (body.isEmpty()) {
            return new long[0];
        }

        String[] tokens = body.split(",");
        long[] values = new long[tokens.length];
        for (int i = 0; i < tokens.length; i++) {
            String token = tokens[i].trim();
            try {
                values[i] = Long.parseLong(token);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(
                        "Invalid histogram payload: non-numeric value '" + token + "' in field '"
                                + fieldName
                                + "'",
                        e);
            }
        }
        return values;
    }

    /**
     * Returns the stack distance frequency histogram. Map from bucket index to frequency count.
     */
    public Map<Integer, Long> getHistogram() {
        return new HashMap<>(histogram);
    }

    public int getNumBuckets() {
        return numBuckets;
    }

    public long getMaxStackDistance() {
        return maxStackDistance;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    /**
     * Merges multiple stack histograms into a single histogram. Frequencies for corresponding
     * bucket indices are summed (Quickmrc Section 3.4.2).
     *
     * <p>All histograms must have the same number of buckets (H) and max stack distance (Dmax).
     * The merged histogram will have:
     * <ul>
     *   <li>Same H (numBuckets) as input histograms</li>
     *   <li>Max Dmax (maxStackDistance) across all input histograms</li>
     *   <li>Sum of frequencies for each bucket index</li>
     *   <li>Total number of partitions equal to sum of all input partitions</li>
     * </ul>
     *
     * @param histograms List of histograms to merge (must not be empty)
     * @return A new merged histogram
     * @throws IllegalArgumentException if histograms are empty, or if histograms have different
     *     configurations (numBuckets or maxStackDistance)
     */
    public static StackHistogram merge(List<StackHistogram> histograms) {
        if (histograms == null || histograms.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty list of histograms");
        }

        if (histograms.size() == 1) {
            return histograms.get(0);
        }

        // Validate all histograms have same configuration
        int numBuckets = histograms.get(0).numBuckets;
        long maxStackDistance = histograms.get(0).maxStackDistance;
        int totalPartitions = 0;

        for (StackHistogram h : histograms) {
            if (h.numBuckets != numBuckets) {
                throw new IllegalArgumentException(
                        String.format(
                                "Cannot merge histograms with different number of buckets: %d vs %d",
                                numBuckets, h.numBuckets));
            }
            maxStackDistance = Math.max(maxStackDistance, h.maxStackDistance);
            totalPartitions += h.numPartitions;
        }

        // Merge histograms by summing frequencies for corresponding bucket indices
        Map<Integer, Long> mergedHistogram = new HashMap<>();
        for (StackHistogram h : histograms) {
            h.histogram.forEach(
                    (bucketIndex, count) ->
                            mergedHistogram.merge(bucketIndex, count, Long::sum));
        }

        return new StackHistogram(mergedHistogram, numBuckets, maxStackDistance, totalPartitions);
    }

    /**
     * Merges this histogram with another histogram. Frequencies for corresponding bucket indices
     * are summed.
     *
     * @param other The other histogram to merge with
     * @return A new merged histogram
     * @throws IllegalArgumentException if histograms have different configurations
     */
    public StackHistogram merge(StackHistogram other) {
        return merge(Arrays.asList(this, other));
    }

    /**
     * Returns the total frequency count across all buckets.
     *
     * @return Sum of all frequency counts
     */
    public long getTotalFrequency() {
        return histogram.values().stream().mapToLong(Long::longValue).sum();
    }

    /**
     * Returns the frequency count for a specific bucket index.
     *
     * @param bucketIndex The bucket index (0 to H-1)
     * @return The frequency count for that bucket, or 0 if the bucket doesn't exist
     */
    public long getFrequency(int bucketIndex) {
        return histogram.getOrDefault(bucketIndex, 0L);
    }

    @Override
    public String toString() {
        return String.format(
                "StackHistogram{numBuckets=%d, maxStackDistance=%d, numPartitions=%d, totalFrequency=%d}",
                numBuckets, maxStackDistance, numPartitions, getTotalFrequency());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        StackHistogram that = (StackHistogram) obj;
        return numBuckets == that.numBuckets
                && maxStackDistance == that.maxStackDistance
                && numPartitions == that.numPartitions
                && histogram.equals(that.histogram);
    }

    @Override
    public int hashCode() {
        int result = histogram.hashCode();
        result = 31 * result + numBuckets;
        result = 31 * result + Long.hashCode(maxStackDistance);
        result = 31 * result + numPartitions;
        return result;
    }
}
