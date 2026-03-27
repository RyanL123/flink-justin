package org.apache.flink.runtime.a4s.core;

import org.apache.flink.util.jackson.JacksonMapperFactory;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/** Stack distance histogram with fixed-order bucket counts. */
@JsonIgnoreProperties(ignoreUnknown = true)
public class StackDistanceHistogram implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final ObjectMapper OBJECT_MAPPER = JacksonMapperFactory.createObjectMapper();

    private final long[] bucketCounts;
    private final int numPartitions;
    private final long bucketSizeScaling;
    private final long cacheItemSizeBytes;

    @JsonCreator
    public StackDistanceHistogram(
            @JsonProperty("bucketCounts") long[] bucketCounts,
            @JsonProperty("numPartitions") int numPartitions,
            @JsonProperty("bucketSizeScaling") long bucketSizeScaling,
            @JsonProperty("cacheItemSizeBytes") long cacheItemSizeBytes) {
        validateCounts(bucketCounts);
        validateBucketSizeScaling(bucketSizeScaling);
        validateCacheItemSizeBytes(cacheItemSizeBytes);
        this.bucketCounts = Arrays.copyOf(bucketCounts, bucketCounts.length);
        this.numPartitions = numPartitions;
        this.bucketSizeScaling = bucketSizeScaling;
        this.cacheItemSizeBytes = cacheItemSizeBytes;
    }

    public String toMetricString() {
        try {
            return OBJECT_MAPPER.writeValueAsString(this);
        } catch (Exception e) {
            throw new IllegalArgumentException("Failed to serialize stack distance histogram", e);
        }
    }

    public static StackDistanceHistogram fromMetricString(String serializedHistogram) {
        if (serializedHistogram == null || serializedHistogram.trim().isEmpty()) {
            throw new IllegalArgumentException("Serialized histogram must not be null or empty");
        }

        try {
            StackDistanceHistogram histogram =
                    OBJECT_MAPPER.readValue(serializedHistogram, StackDistanceHistogram.class);
            if (histogram == null) {
                throw new IllegalArgumentException("Invalid histogram payload: empty object");
            }
            return histogram;
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Invalid histogram payload: expected JSON StackDistanceHistogram object", e);
        }
    }

    private static void validateCounts(long[] counts) {
        if (counts == null) {
            throw new IllegalArgumentException("Bucket counts must not be null");
        }
        for (long count : counts) {
            if (count < 0L) {
                throw new IllegalArgumentException("Bucket counts must be non-negative");
            }
        }
    }

    private static void validateBucketSizeScaling(long bucketSizeScaling) {
        if (bucketSizeScaling <= 0L) {
            throw new IllegalArgumentException("bucketSizeScaling must be > 0");
        }
    }

    private static void validateCacheItemSizeBytes(long cacheItemSizeBytes) {
        if (cacheItemSizeBytes <= 0L) {
            throw new IllegalArgumentException("cacheItemSizeBytes must be > 0");
        }
    }

    public long[] getBucketCounts() {
        return Arrays.copyOf(bucketCounts, bucketCounts.length);
    }

    public long[] toBucketCountsArray() {
        return getBucketCounts();
    }

    public int getNumBuckets() {
        return bucketCounts.length;
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public long getBucketSizeScaling() {
        return bucketSizeScaling;
    }

    public long getCacheItemSizeBytes() {
        return cacheItemSizeBytes;
    }

    /**
     * Deterministic fingerprint for correlating logs across TaskManager histogram export and
     * JobManager aggregation (FNV-1a 64-bit over bucket counts and partition count).
     */
    public long getHistogramFingerprint() {
        long h = 0xcbf29ce484222325L;
        final long prime = 0x100000001b3L;
        for (long count : bucketCounts) {
            h ^= count;
            h *= prime;
        }
        h ^= (long) numPartitions;
        h *= prime;
        h ^= bucketSizeScaling;
        h *= prime;
        h ^= cacheItemSizeBytes;
        h *= prime;
        return h;
    }

    public static StackDistanceHistogram merge(List<StackDistanceHistogram> histograms) {
        if (histograms == null || histograms.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty list of histograms");
        }

        if (histograms.size() == 1) {
            return histograms.get(0);
        }

        int numBuckets = 0;
        for (StackDistanceHistogram histogram : histograms) {
            numBuckets = Math.max(numBuckets, histogram.getNumBuckets());
        }

        long[] mergedCounts = new long[numBuckets];
        long mergedBucketSizeScaling = histograms.get(0).getBucketSizeScaling();
        long mergedCacheItemSizeBytes = histograms.get(0).getCacheItemSizeBytes();

        int totalPartitions = histograms.stream().mapToInt(h -> h.getNumPartitions()).sum();
        for (StackDistanceHistogram histogram : histograms) {
            if (histogram.getBucketSizeScaling() != mergedBucketSizeScaling) {
                throw new IllegalArgumentException(
                        "Cannot merge histograms with different bucketSizeScaling values");
            }
            if (histogram.getCacheItemSizeBytes() != mergedCacheItemSizeBytes) {
                throw new IllegalArgumentException(
                        "Cannot merge histograms with different cacheItemSizeBytes values");
            }
            for (int i = 0; i < histogram.getNumBuckets() - 1; i++) {
                mergedCounts[i] += histogram.getFrequency(i);
            }
            // last bucket always stores complete misses (infinite stack distance)
            mergedCounts[numBuckets - 1] += histogram.getFrequency(histogram.getNumBuckets() - 1);
        }

        return new StackDistanceHistogram(
                mergedCounts, totalPartitions, mergedBucketSizeScaling, mergedCacheItemSizeBytes);
    }

    public long getTotalFrequency() {
        long total = 0L;
        for (long count : this.bucketCounts) {
            total += count;
        }
        return total;
    }

    public long getFrequency(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= bucketCounts.length) {
            return 0L;
        }
        return bucketCounts[bucketIndex];
    }

    @Override
    public String toString() {
        return String.format(
                "StackDistanceHistogram{numBuckets=%d, numPartitions=%d, bucketSizeScaling=%d, cacheItemSizeBytes=%d, totalFrequency=%d}",
                getNumBuckets(),
                numPartitions,
                bucketSizeScaling,
                cacheItemSizeBytes,
                getTotalFrequency());
    }
}
