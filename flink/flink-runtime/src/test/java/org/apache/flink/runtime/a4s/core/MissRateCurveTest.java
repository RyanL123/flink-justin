package org.apache.flink.runtime.a4s.core;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MissRateCurveTest {
    @Test
    void testMerge_preservesSumsAndPartitions() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(List.of(3L, 1L, 1L), 1);
        StackDistanceHistogram h2 = new StackDistanceHistogram(List.of(2L, 2L, 1L), 2);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(3, merged.getNumPartitions());
        assertEquals(10L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(3L, merged.getFrequency(1));
        assertEquals(2L, merged.getFrequency(2));
    }

    @Test
    void testMerge_twoHistograms_differentNumBuckets() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(List.of(3L, 1L, 1L), 1);
        StackDistanceHistogram h2 = new StackDistanceHistogram(List.of(2L, 2L), 1);

        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(9L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(3L, merged.getFrequency(2));
    }

    @Test
    void testMerge_twoHistograms_oneEmpty() {
        StackDistanceHistogram h1 = new StackDistanceHistogram(List.of(3L, 1L, 1L), 1);
        StackDistanceHistogram h2 = new StackDistanceHistogram(new ArrayList<>(), 1);
        StackDistanceHistogram merged = StackDistanceHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(5L, merged.getTotalFrequency());
        assertEquals(3L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(1L, merged.getFrequency(2));
    }

    @Test
    void testScaledMRC_horizontalScalingLogic() {
        List<Long> buckets = new ArrayList<>();
        buckets.add(5L);
        buckets.add(3L);
        buckets.add(2L);
        buckets.add(0L);
        int partitions = 2;
        StackDistanceHistogram histogram = new StackDistanceHistogram(buckets, partitions);

        List<MissRateCurve.Point> scaled = MissRateCurve.computeScaledMRC(histogram, 4096L, 1L);
        List<MissRateCurve.Point> unscaled =
                MissRateCurve.computeUnscaledMRC(histogram, 4096L, 1L);

        assertEquals(unscaled.size(), scaled.size());

        List<MissRateCurve.Point> expectedUnscaled =
                List.of(
                        new MissRateCurve.Point(4096L, 0.5),
                        new MissRateCurve.Point(8192L, 0.2),
                        new MissRateCurve.Point(12288L, 0.0));
        List<MissRateCurve.Point> expectedScaled =
                List.of(
                        new MissRateCurve.Point(8192L, 0.5),
                        new MissRateCurve.Point(16384L, 0.2),
                        new MissRateCurve.Point(24576L, 0.0));

        for (int i = 0; i < expectedUnscaled.size(); i++) {
            assertEquals(
                    expectedUnscaled.get(i).getCacheSizeBytes(), unscaled.get(i).getCacheSizeBytes());
            assertEquals(expectedUnscaled.get(i).getMissRate(), unscaled.get(i).getMissRate(), 0.0001);
        }
        for (int i = 0; i < expectedScaled.size(); i++) {
            assertEquals(expectedScaled.get(i).getCacheSizeBytes(), scaled.get(i).getCacheSizeBytes());
            assertEquals(expectedScaled.get(i).getMissRate(), scaled.get(i).getMissRate(), 0.0001);
        }
    }

    @Test
    void testMRC_emptyHistogram() {
        StackDistanceHistogram empty = new StackDistanceHistogram(new ArrayList<>(), 1);
        assertTrue(MissRateCurve.computeUnscaledMRC(empty, 4096L, 1L).isEmpty());
        assertTrue(MissRateCurve.computeScaledMRC(empty, 4096L, 1L).isEmpty());
    }

    @Test
    void testFromSerializedValue_parsesRocksDBPayload() {
        String payload = "[5,7,11]";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromSerializedValue(payload);

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(1, histogram.getNumPartitions());
        assertEquals(5L, histogram.getFrequency(0));
        assertEquals(7L, histogram.getFrequency(1));
        assertEquals(11L, histogram.getFrequency(2));
        assertEquals(23L, histogram.getTotalFrequency());
    }

    @Test
    void testFromSerializedValue_keepsSparseMapForZeroCountBuckets() {
        String payload = "[0,10,0]";

        StackDistanceHistogram histogram = StackDistanceHistogram.fromSerializedValue(payload);
        List<Long> buckets = histogram.getBucketCounts();

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(3, buckets.size());
        assertEquals(10L, buckets.get(1));
        assertEquals(0L, buckets.get(0));
        assertEquals(0L, buckets.get(2));
    }

    @Test
    void testFromSerializedValue_throwsWhenCountsLengthIsInvalid() {
        String payload = "[5,-1,11]";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackDistanceHistogram.fromSerializedValue(payload));
    }
}
