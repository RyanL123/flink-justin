package org.apache.flink.runtime.standalone_stackhistogram;

import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.flink.runtime.standalone_stackhistogram.StackHistogram.Bucket;
class QuickMRCTest {
    /**
     * Test 1: Verify Merge Logic
     * Ensures bucket counts and partition counts are summed correctly.
     */
    @Test
    void testMerge_preservesSumsAndPartitions() {
        StackHistogram h1 = new StackHistogram(
            List.of(
                new Bucket(1L, 3L),
                new Bucket(2L, 1L),
                new Bucket(3L, 1L)
            ), 1);
        StackHistogram h2 = new StackHistogram(
            List.of(
                new Bucket(1L, 2L),
                new Bucket(2L, 2L),
                new Bucket(3L, 1L)
            ), 2);

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
        List<Bucket> buckets = new ArrayList<>();
        buckets.add(new Bucket(1L, 5L)); // 5 hits in first bucket
        buckets.add(new Bucket(2L, 3L)); // 3 hits in second bucket
        buckets.add(new Bucket(3L, 2L)); // 2 hits in third bucket
        int partitions = 2;
        StackHistogram histogram = new StackHistogram(buckets, partitions);

        List<QuickMRC.MRCPoint> scaled = QuickMRC.computeScaledMRC(histogram);
        List<QuickMRC.MRCPoint> unscaled = QuickMRC.computeUnscaledMRC(histogram);

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
        assertTrue(QuickMRC.computeUnscaledMRC(empty).isEmpty());
        assertTrue(QuickMRC.computeScaledMRC(empty).isEmpty());
    }

    @Test
    void testFromSerializedValue_parsesRocksDBPayload() {
        String payload = "{\"boundaries\":[32,64,96],\"counts\":[5,7,11]}";

        StackHistogram histogram = StackHistogram.fromSerializedValue(payload);

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(96L, histogram.getRightBoundaryInclusive(2));
        
        assertEquals(1, histogram.getNumPartitions());
        assertEquals(5L, histogram.getFrequency(0));
        assertEquals(7L, histogram.getFrequency(1));
        assertEquals(11L, histogram.getFrequency(2));
        assertEquals(23L, histogram.getTotalFrequency());
    }

    @Test
    void testFromSerializedValue_keepsSparseMapForZeroCountBuckets() {
        String payload = "{\"boundaries\":[32,64,96],\"counts\":[0,10,0]}";

        StackHistogram histogram = StackHistogram.fromSerializedValue(payload);
        List<Bucket> buckets = histogram.getBuckets();

        assertEquals(3, histogram.getNumBuckets());
        assertEquals(3, buckets.size());
        assertEquals(10L, buckets.get(1).getCount());
        assertEquals(0L, buckets.get(0).getCount());
        assertEquals(0L, buckets.get(2).getCount());
    }

    @Test
    void testFromSerializedValue_throwsWhenCountsLengthIsInvalid() {
        String payload = "{\"boundaries\":[32,64,96],\"counts\":[5,7,11,13]}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackHistogram.fromSerializedValue(payload));
    }

    @Test
    void testFromSerializedValue_throwsWhenFieldMissing() {
        String payload = "{\"boundaries\":[32,64,96]}";

        assertThrows(
                IllegalArgumentException.class,
                () -> StackHistogram.fromSerializedValue(payload));
    }
}