package org.apache.flink.runtime.standalone_stackhistogram;

import java.util.ArrayList;
import java.util.List;

/** Stack histogram with bucket entries [x_i, y_i], where x_i is right boundary (inclusive). */
public class StackHistogram {

    public static final int DEFAULT_NUM_BUCKETS = 50;

    public static final class Bucket {
        private final long rightBoundaryInclusive;
        private long count;

        public Bucket(long rightBoundaryInclusive, long count) {
            this.rightBoundaryInclusive = rightBoundaryInclusive;
            this.count = count;
        }

        public long getRightBoundaryInclusive() {
            return rightBoundaryInclusive;
        }

        public long getCount() {
            return count;
        }

        public void addCount(long count) {
            this.count += count;
        }
    }

    private final List<Bucket> buckets;
    private final int numPartitions;

    public StackHistogram(
        List<Bucket> buckets,
        int numPartitions) {
        this.buckets = List.copyOf(buckets);
        this.numPartitions = numPartitions;
    }

    public static StackHistogram fromSerializedValue(String serializedHistogram) {
        if (serializedHistogram == null || serializedHistogram.trim().isEmpty()) {
            throw new IllegalArgumentException("Serialized histogram must not be null or empty");
        }

        long[] boundaries = parseLongArrayField(serializedHistogram, "boundaries");
        long[] counts = parseLongArrayField(serializedHistogram, "counts");

        if (counts.length != boundaries.length) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid histogram payload: counts length (%d) must equal boundaries length (%d)",
                            counts.length, boundaries.length));
        }

        List<Bucket> parsedBuckets = new ArrayList<>(counts.length);
        for (int i = 0; i < counts.length; i++) {
            if (counts[i] < 0L) {
                throw new IllegalArgumentException(
                        "Invalid histogram payload: counts must be non-negative");
            }
            parsedBuckets.add(new Bucket(boundaries[i], counts[i]));
        }
        return new StackHistogram(parsedBuckets, 1);
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

    public List<Bucket> getBuckets() {
        return buckets;
    }

    public int getNumBuckets() {
        return buckets.size();
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
        
        List<Bucket> buckets = List.copyOf(histograms.get(0).getBuckets());
        int totalPartitions = histograms.stream().mapToInt(h -> h.getNumPartitions()).sum();

        for (int i = 0; i < buckets.size(); i++) {
            Bucket bucket = buckets.get(i);
            for (StackHistogram h : histograms.subList(1, histograms.size())) {
                bucket.addCount(h.getBuckets().get(i).getCount());
            }
        }
        return new StackHistogram(buckets, totalPartitions);
    }

    public long getTotalFrequency() {
        long total = 0L;
        for (Bucket bucket : buckets) {
            total += bucket.getCount();
        }
        return total;
    }

    public long getFrequency(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= buckets.size()) {
            return 0L;
        }
        return buckets.get(bucketIndex).getCount();
    }

    public long getRightBoundaryInclusive(int bucketIndex) {
        if (bucketIndex < 0 || bucketIndex >= buckets.size()) {
            return 0L;
        }
        return buckets.get(bucketIndex).getRightBoundaryInclusive();
    }

    @Override
    public String toString() {
        return String.format(
                "StackHistogram{numBuckets=%d, numPartitions=%d, totalFrequency=%d}",
                getNumBuckets(), numPartitions, getTotalFrequency());
    }
}
