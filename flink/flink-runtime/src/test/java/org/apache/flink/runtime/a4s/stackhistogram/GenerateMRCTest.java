package org.apache.flink.runtime.a4s.stackhistogram;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class GenerateMRCTest {
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

    @Test
    void testMerge_twoHistograms_differentNumBuckets() {
        StackHistogram h1 = new StackHistogram(List.of(3L, 1L, 1L), 1);
        StackHistogram h2 = new StackHistogram(List.of(2L, 2L), 1);

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

        assertEquals(2, merged.getNumPartitions());
        assertEquals(9L, merged.getTotalFrequency());
        assertEquals(5L, merged.getFrequency(0));
        assertEquals(1L, merged.getFrequency(1));
        assertEquals(3L, merged.getFrequency(2));
    }

    @Test
    void testMerge_twoHistograms_oneEmpty() {
        StackHistogram h1 = new StackHistogram(List.of(3L, 1L, 1L), 1);
        StackHistogram h2 = new StackHistogram(new ArrayList<>(), 1);
        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

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
        StackHistogram histogram = new StackHistogram(buckets, partitions);

        List<GenerateMRC.MRCPoint> scaled = GenerateMRC.computeScaledMRC(histogram, 4096L, 1L);
        List<GenerateMRC.MRCPoint> unscaled =
                GenerateMRC.computeUnscaledMRC(histogram, 4096L, 1L);

        assertEquals(unscaled.size(), scaled.size());

        List<GenerateMRC.MRCPoint> expectedUnscaled =
                List.of(
                        new GenerateMRC.MRCPoint(4096L, 0.5),
                        new GenerateMRC.MRCPoint(8192L, 0.2),
                        new GenerateMRC.MRCPoint(12288L, 0.0));
        List<GenerateMRC.MRCPoint> expectedScaled =
                List.of(
                        new GenerateMRC.MRCPoint(8192L, 0.5),
                        new GenerateMRC.MRCPoint(16384L, 0.2),
                        new GenerateMRC.MRCPoint(24576L, 0.0));

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
        StackHistogram empty = new StackHistogram(new ArrayList<>(), 1);
        assertTrue(GenerateMRC.computeUnscaledMRC(empty, 4096L, 1L).isEmpty());
        assertTrue(GenerateMRC.computeScaledMRC(empty, 4096L, 1L).isEmpty());
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

        assertThrows(
                IllegalArgumentException.class, () -> StackHistogram.fromSerializedValue(payload));
    }
}

