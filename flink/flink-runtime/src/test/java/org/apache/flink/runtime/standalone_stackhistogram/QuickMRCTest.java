package org.apache.flink.runtime.standalone_stackhistogram;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QuickMRCTest {
    /**
     * Test 1: Verify Merge Logic
     * Ensures bucket counts and partition counts are summed correctly.
     */
    @Test
    void testMerge_preservesSumsAndPartitions() {
        StackHistogram h1 = new StackHistogram(List.of(3L, 1L, 1L), 1);
        StackHistogram h2 = new StackHistogram(List.of(2L, 2L, 1L), 2);

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

        assertEquals(3, merged.getNumPartitions());
        assertEquals(10L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(3L, merged.getFrequency(1));
        assertEquals(2L, merged.getFrequency(2));
    }

    /**
     * Test 3: Verify Horizontal Scaling Logic
     * Explicitly checks that Cache Size is multiplied by the factor.
     */
    @Test
    void testScaledMRC_horizontalScalingLogic() {
        List<Long> buckets = new ArrayList<>();
        buckets.add(5L); // 5 hits in first bucket
        buckets.add(3L); // 3 hits in second bucket
        buckets.add(2L); // 2 hits in third bucket
        int partitions = 2;
        StackHistogram histogram = new StackHistogram(buckets, partitions);

        List<QuickMRC.MRCPoint> scaled = QuickMRC.computeScaledMRC(histogram, 4096L, 1L);
        List<QuickMRC.MRCPoint> unscaled = QuickMRC.computeUnscaledMRC(histogram, 4096L, 1L);

        assertEquals(unscaled.size(), scaled.size());

        List<QuickMRC.MRCPoint> expectedUnscaled = List.of(
            new QuickMRC.MRCPoint(4096L, 0.5),
            new QuickMRC.MRCPoint(8192L, 0.2),
            new QuickMRC.MRCPoint(12288L, 0.0)
        );
        List<QuickMRC.MRCPoint> expectedScaled = List.of(
            new QuickMRC.MRCPoint(8192L, 0.5),
            new QuickMRC.MRCPoint(16384L, 0.2),
            new QuickMRC.MRCPoint(24576L, 0.0)
        );

        for (int i = 0; i < expectedUnscaled.size(); i++) {
            assertEquals(expectedUnscaled.get(i).getCacheSizeBytes(), unscaled.get(i).getCacheSizeBytes());
            assertEquals(expectedUnscaled.get(i).getMissRate(), unscaled.get(i).getMissRate(), 0.0001);
        }
        for (int i = 0; i < expectedScaled.size(); i++) {
            assertEquals(expectedScaled.get(i).getCacheSizeBytes(), scaled.get(i).getCacheSizeBytes());
            assertEquals(expectedScaled.get(i).getMissRate(), scaled.get(i).getMissRate(), 0.0001);
        }
    }

    /**
     * Test 5: Edge Case
     * Empty histogram should not crash.
     */
    @Test
    void testMRC_emptyHistogram() {
        StackHistogram empty = new StackHistogram(new ArrayList<>(), 1);
        assertTrue(QuickMRC.computeUnscaledMRC(empty, 4096L, 1L).isEmpty());
        assertTrue(QuickMRC.computeScaledMRC(empty, 4096L, 1L).isEmpty());
    }

    @Test
    void testFromSerializedValue_parsesRocksDBPayload() {
        String payload = "[5,7,11]";

        StackHistogram histogram = StackHistogram.fromSerializedValue(payload);

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

        StackHistogram histogram = StackHistogram.fromSerializedValue(payload);
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

        assertThrows(IllegalArgumentException.class, () -> StackHistogram.fromSerializedValue(payload));
    }
}