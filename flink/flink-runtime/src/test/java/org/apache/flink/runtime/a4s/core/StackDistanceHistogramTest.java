package org.apache.flink.runtime.a4s.core;

import java.util.List;
import java.util.Arrays;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class StackDistanceHistogramTest {
    @Test
    void testMerge_preservesSumsAndPartitions() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(new long[] {3L, 1L, 1L}, 1, 2L);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[] {2L, 2L, 1L}, 2, 2L);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(3, merged.getNumPartitions());
        assertEquals(10L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(3L, merged.getFrequency(1));
        assertEquals(2L, merged.getFrequency(2));
        assertEquals(2L, merged.getBucketSizeScaling());
    }

    @Test
    void testMerge_twoHistograms_differentNumBuckets() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(new long[] {3L, 1L, 1L}, 1, 3L);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[] {2L, 2L}, 1, 3L);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(9L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(3L, merged.getFrequency(2));
        assertEquals(3L, merged.getBucketSizeScaling());
    }

    @Test
    void testMerge_twoHistograms_oneEmpty() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(new long[] {3L, 1L, 1L}, 1, 4L);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[0], 1, 4L);
        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(5L, merged.getTotalFrequency());
        assertEquals(3L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(1L, merged.getFrequency(2));
        assertEquals(4L, merged.getBucketSizeScaling());
    }

    @Test
    void testMRC_emptyHistogram() {
        StackDistanceHistogram empty = new StackDistanceHistogram(new long[0], 1, 1L);
        assertTrue(MissRateCurve.fromStackDistanceHistogram(empty, 4096L).getPoints().isEmpty());
    }

    @Test
    void testFromSerializedValue_parsesRocksDBPayload() {
        String payload =
                "{\"bucketCounts\":[5,7,11],\"numPartitions\":1,\"bucketSizeScaling\":2,\"scopeInfo\":null,\"name\":null}";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromMetricString(payload);

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(1, histogram.getNumPartitions());
        assertEquals(5L, histogram.getFrequency(0));
        assertEquals(7L, histogram.getFrequency(1));
        assertEquals(11L, histogram.getFrequency(2));
        assertEquals(23L, histogram.getTotalFrequency());
        assertEquals(2L, histogram.getBucketSizeScaling());
    }

    @Test
    void testFromSerializedValue_keepsSparseMapForZeroCountBuckets() {
        String payload =
                "{\"bucketCounts\":[0,10,0],\"numPartitions\":1,\"bucketSizeScaling\":4,\"scopeInfo\":null,\"name\":null}";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromMetricString(payload);
        long[] buckets = histogram.getBucketCounts();

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(3, buckets.length);
        assertEquals(10L, buckets[1]);
        assertEquals(0L, buckets[0]);
        assertEquals(0L, buckets[2]);
        assertEquals(4L, histogram.getBucketSizeScaling());
    }

    @Test
    void testFromSerializedValue_throwsWhenCountsLengthIsInvalid() {
        String payload =
                "{\"bucketCounts\":[5,-1,11],\"numPartitions\":1,\"bucketSizeScaling\":1,\"scopeInfo\":null,\"name\":null}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.fromMetricString(payload));
    }

    @Test
    void testFromSerializedValue_throwsWhenBucketSizeScalingMissing() {
        String payload = "{\"bucketCounts\":[5,7,11],\"numPartitions\":1}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.fromMetricString(payload));
    }

    @Test
    void testMerge_throwsWhenBucketSizeScalingDiffers() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(new long[] {3L, 1L, 1L}, 1, 1L);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[] {2L, 2L, 1L}, 2, 2L);

        assertThrows(
                IllegalArgumentException.class, () -> StackDistanceHistogram.merge(List.of(h1, h2)));
    }

    @Test
    void testMetricRoundTrip_serializesEntireHistogramObject() {
        StackDistanceHistogram histogram =
                new StackDistanceHistogram(new long[] {1L, 2L, 3L}, 4, 8L);

        StackDistanceHistogram parsed =
                StackDistanceHistogram.fromMetricString(histogram.toMetricString());

        assertTrue(Arrays.equals(new long[] {1L, 2L, 3L}, parsed.getBucketCounts()));
        assertEquals(4, parsed.getNumPartitions());
        assertEquals(8L, parsed.getBucketSizeScaling());
    }
}
