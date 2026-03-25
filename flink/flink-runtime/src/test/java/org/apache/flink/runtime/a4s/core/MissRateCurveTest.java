package org.apache.flink.runtime.a4s.core;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MissRateCurveTest {
    @Test
    void testMerge_preservesSumsAndPartitions() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(new long[] {3L, 1L, 1L}, 1);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[] {2L, 2L, 1L}, 2);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(3, merged.getNumPartitions());
        assertEquals(10L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(3L, merged.getFrequency(1));
        assertEquals(2L, merged.getFrequency(2));
    }

    @Test
    void testMerge_twoHistograms_differentNumBuckets() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(new long[] {3L, 1L, 1L}, 1);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[] {2L, 2L}, 1);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(9L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(3L, merged.getFrequency(2));
    }

    @Test
    void testMerge_twoHistograms_oneEmpty() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(new long[] {3L, 1L, 1L}, 1);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new long[0], 1);
        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(5L, merged.getTotalFrequency());
        assertEquals(3L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(1L, merged.getFrequency(2));
    }

    @Test
    void testScaledMRC_horizontalScalingLogic() {
        long[] buckets = new long[] {5L, 3L, 2L, 0L};
        int partitions = 2;
        StackDistanceHistogram histogram = new StackDistanceHistogram(buckets, partitions);

        MissRateCurve scaled = MissRateCurve.fromStackDistanceHistogram(histogram, 4096L, 1L);

        List<MissRateCurve.Point> expectedScaled =
                List.of(
                        new MissRateCurve.Point(8192L, 0.5),
                        new MissRateCurve.Point(16384L, 0.2),
                        new MissRateCurve.Point(24576L, 0.0));

        for (int i = 0; i < expectedScaled.size(); i++) {
            assertEquals(
                    expectedScaled.get(i).getCacheSizeBytes(),
                    scaled.getPoints().get(i).getCacheSizeBytes());
            assertEquals(
                    expectedScaled.get(i).getMissRate(),
                    scaled.getPoints().get(i).getMissRate(),
                    0.0001);
        }
    }

    @Test
    void testMRC_emptyHistogram() {
        StackDistanceHistogram empty = new StackDistanceHistogram(new long[0], 1);
        assertTrue(
                MissRateCurve.fromStackDistanceHistogram(empty, 4096L, 1L).getPoints().isEmpty());
    }

    @Test
    void testFromSerializedValue_parsesRocksDBPayload() {
        String payload =
                "{\"bucketCounts\":[5,7,11],\"numPartitions\":1,\"scopeInfo\":null,\"name\":null}";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromMetricString(payload);

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(1, histogram.getNumPartitions());
        assertEquals(5L, histogram.getFrequency(0));
        assertEquals(7L, histogram.getFrequency(1));
        assertEquals(11L, histogram.getFrequency(2));
        assertEquals(23L, histogram.getTotalFrequency());
    }

    @Test
    void testFromSerializedValue_keepsSparseMapForZeroCountBuckets() {
        String payload =
                "{\"bucketCounts\":[0,10,0],\"numPartitions\":1,\"scopeInfo\":null,\"name\":null}";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromMetricString(payload);
        long[] buckets = histogram.getBucketCounts();

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(3, buckets.length);
        assertEquals(10L, buckets[1]);
        assertEquals(0L, buckets[0]);
        assertEquals(0L, buckets[2]);
    }

    @Test
    void testFromSerializedValue_throwsWhenCountsLengthIsInvalid() {
        String payload =
                "{\"bucketCounts\":[5,-1,11],\"numPartitions\":1,\"scopeInfo\":null,\"name\":null}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.fromMetricString(payload));
    }

    @Test
    void testMetricRoundTrip_serializesEntireHistogramObject() {
        StackDistanceHistogram histogram = new StackDistanceHistogram(new long[] {1L, 2L, 3L}, 4);

        StackDistanceHistogram parsed =
                StackDistanceHistogram.fromMetricString(histogram.toMetricString());

        assertTrue(Arrays.equals(new long[] {1L, 2L, 3L}, parsed.getBucketCounts()));
        assertEquals(4, parsed.getNumPartitions());
    }
}
