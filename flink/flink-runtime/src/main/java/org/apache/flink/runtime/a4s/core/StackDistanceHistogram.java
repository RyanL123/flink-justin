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

    @JsonCreator
    public StackDistanceHistogram(
            @JsonProperty("bucketCounts") long[] bucketCounts,
            @JsonProperty("numPartitions") int numPartitions) {
        validateCounts(bucketCounts);
        this.bucketCounts = Arrays.copyOf(bucketCounts, bucketCounts.length);
        this.numPartitions = numPartitions;
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
                throw new IllegalArgumentException(
                        "Bucket counts must be non-negative");
            }
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

        int totalPartitions = histograms.stream().mapToInt(h -> h.getNumPartitions()).sum();
        for (StackDistanceHistogram histogram : histograms) {
            for (int i = 0; i < histogram.getNumBuckets() - 1; i++) {
                mergedCounts[i] += histogram.getFrequency(i);
            }
            // last bucket always stores complete misses (infinite stack distance)
            mergedCounts[numBuckets - 1] += histogram.getFrequency(histogram.getNumBuckets() - 1);
        }

        return new StackDistanceHistogram(mergedCounts, totalPartitions);
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
                "StackDistanceHistogram{numBuckets=%d, numPartitions=%d, totalFrequency=%d}",
                getNumBuckets(), numPartitions, getTotalFrequency());
    }
}
