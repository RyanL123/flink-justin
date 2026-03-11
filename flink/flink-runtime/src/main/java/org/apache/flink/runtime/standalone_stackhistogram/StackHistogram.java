package org.apache.flink.runtime.standalone_stackhistogram;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Stack histogram with fixed-order bucket counts. */
public class StackHistogram {
    private final List<Long> bucketCounts;
    private final int numPartitions;

    public StackHistogram(List<Long> bucketCounts, int numPartitions) {
        validateCounts(bucketCounts);
        this.bucketCounts = List.copyOf(bucketCounts);
        this.numPartitions = numPartitions;
    }

    public static StackHistogram fromSerializedValue(String serializedHistogram) {
        if (serializedHistogram == null || serializedHistogram.trim().isEmpty()) {
            throw new IllegalArgumentException("Serialized histogram must not be null or empty");
        }

        long[] counts = parseLongArray(serializedHistogram);

        List<Long> parsedCounts = new ArrayList<>(counts.length);
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] < 0L) {
                throw new IllegalArgumentException(
                        "Invalid histogram payload: counts must be non-negative");
            }
            parsedCounts.add(counts[i]);
        }
        return new StackHistogram(parsedCounts, 1);
    }

    private static long[] parseLongArray(String serializedArray) {
        String trimmed = serializedArray.trim();
        if (!trimmed.startsWith("[") || !trimmed.endsWith("]")) {
            throw new IllegalArgumentException(
                    "Invalid histogram payload: expected JSON array of counts");
        }

        String body = trimmed.substring(1, trimmed.length() - 1).trim();
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
                        "Invalid histogram payload: non-numeric value '" + token + "'",
                        e);
            }
        }
        return values;
    }

    private static void validateCounts(List<Long> counts) {
        if (counts == null) {
            throw new IllegalArgumentException("Bucket counts must not be null");
        }
        for (Long count : counts) {
            if (count == null || count < 0L) {
                throw new IllegalArgumentException(
                        "Bucket counts must be non-null and non-negative");
            }
        }
    }

    public List<Long> getBucketCounts() {
        return bucketCounts;
    }

    public int getNumBuckets() {
        return bucketCounts.size();
    }

    public int getNumPartitions() {
        return numPartitions;
    }

    public static StackHistogram merge(List<StackHistogram> histograms) {
        if (histograms == null || histograms.isEmpty()) {
            throw new IllegalArgumentException("Cannot merge empty list of histograms");
        }

        if (histograms.size() == 1) {
            return histograms.get(0);
        }

        int numBuckets = 0;
        for (StackHistogram histogram : histograms) {
            numBuckets = Math.max(numBuckets, histogram.getNumBuckets());
        }

        List<Long> mergedCounts = new ArrayList<>(Collections.nCopies(numBuckets, 0L));

        int totalPartitions = histograms.stream().mapToInt(h -> h.getNumPartitions()).sum();
        for (StackHistogram histogram : histograms) {
            for (int i = 0; i < histogram.getNumBuckets(); i++) {
                mergedCounts.set(i, mergedCounts.get(i) + histogram.getFrequency(i));
            }
        }
        return new StackHistogram(mergedCounts, totalPartitions);
    }

    public long getTotalFrequency() {
        long total = 0L;
        for (long count : bucketCounts) {
            total += count;
        }
        return total;
    }

    public long getFrequency(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= bucketCounts.size()) {
            return 0L;
        }
        return bucketCounts.get(bucketIndex);
    }

    @Override
    public String toString() {
        return String.format(
                "StackHistogram{numBuckets=%d, numPartitions=%d, totalFrequency=%d}",
                getNumBuckets(), numPartitions, getTotalFrequency());
    }
}
