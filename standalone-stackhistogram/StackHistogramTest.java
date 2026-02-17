/*
 * Unit tests for StackHistogram merging logic.
 * 
 * Tests are based on Quickmrc design (Section 3.4.2) from the A4S paper.
 */

import org.junit.jupiter.api.Test;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for StackHistogram merging functionality.
 */
class StackHistogramTest {

    private static final int NUM_BUCKETS = 50;
    private static final long MAX_STACK_DISTANCE = 500L;

    /**
     * Creates a sample stack histogram with the given bucket frequencies.
     *
     * @param bucketFrequencies Map from bucket index to frequency count
     * @return A new StackHistogram with default configuration
     */
    private static StackHistogram createHistogram(Map<Integer, Long> bucketFrequencies) {
        return new StackHistogram(
                new HashMap<>(bucketFrequencies), NUM_BUCKETS, MAX_STACK_DISTANCE, 1);
    }

    /**
     * Creates a sample stack histogram with custom configuration.
     */
    private static StackHistogram createHistogram(
            Map<Integer, Long> bucketFrequencies,
            int numBuckets,
            long maxStackDistance,
            int numPartitions) {
        return new StackHistogram(
                new HashMap<>(bucketFrequencies), numBuckets, maxStackDistance, numPartitions);
    }

    // ========== Sample Data ==========

    /**
     * Sample histogram from Task Manager 1, Partition 0.
     * Bucket 0: 100 accesses, Bucket 1: 50 accesses, Bucket 2: 25 accesses
     */
    private static StackHistogram createSampleHistogram1() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 100L);
        buckets.put(1, 50L);
        buckets.put(2, 25L);
        buckets.put(5, 10L);
        return createHistogram(buckets);
    }

    /**
     * Sample histogram from Task Manager 1, Partition 1.
     * Bucket 0: 80 accesses, Bucket 1: 60 accesses, Bucket 3: 30 accesses
     */
    private static StackHistogram createSampleHistogram2() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 80L);
        buckets.put(1, 60L);
        buckets.put(3, 30L);
        buckets.put(5, 5L);
        return createHistogram(buckets);
    }

    /**
     * Sample histogram from Task Manager 2, Partition 0.
     * Bucket 0: 120 accesses, Bucket 1: 40 accesses, Bucket 4: 20 accesses
     */
    private static StackHistogram createSampleHistogram3() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 120L);
        buckets.put(1, 40L);
        buckets.put(4, 20L);
        return createHistogram(buckets);
    }

    /**
     * Sample histogram with different bucket configuration (should fail to merge).
     */
    private static StackHistogram createSampleHistogramDifferentBuckets() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 50L);
        return createHistogram(buckets, 100, MAX_STACK_DISTANCE, 1); // Different numBuckets
    }

    /**
     * Sample histogram with different max stack distance (should fail to merge).
     */
    private static StackHistogram createSampleHistogramDifferentDmax() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 50L);
        return createHistogram(buckets, NUM_BUCKETS, 1000L, 1); // Different maxStackDistance
    }

    // ========== Tests ==========

    @Test
    void testCreateHistogram() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 100L);
        buckets.put(1, 50L);

        StackHistogram histogram = createHistogram(buckets);

        assertEquals(NUM_BUCKETS, histogram.getNumBuckets());
        assertEquals(MAX_STACK_DISTANCE, histogram.getMaxStackDistance());
        assertEquals(1, histogram.getNumPartitions());
        assertEquals(100L, histogram.getFrequency(0));
        assertEquals(50L, histogram.getFrequency(1));
        assertEquals(0L, histogram.getFrequency(2)); // Non-existent bucket
        assertEquals(150L, histogram.getTotalFrequency());
    }

    @Test
    void testMergeTwoHistograms_sumsCorrespondingBuckets() {
        StackHistogram h1 = createSampleHistogram1();
        StackHistogram h2 = createSampleHistogram2();

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

        // Bucket 0: 100 + 80 = 180
        assertEquals(180L, merged.getFrequency(0));
        // Bucket 1: 50 + 60 = 110
        assertEquals(110L, merged.getFrequency(1));
        // Bucket 2: 25 + 0 = 25
        assertEquals(25L, merged.getFrequency(2));
        // Bucket 3: 0 + 30 = 30
        assertEquals(30L, merged.getFrequency(3));
        // Bucket 5: 10 + 5 = 15
        assertEquals(15L, merged.getFrequency(5));

        assertEquals(NUM_BUCKETS, merged.getNumBuckets());
        assertEquals(MAX_STACK_DISTANCE, merged.getMaxStackDistance());
        assertEquals(2, merged.getNumPartitions()); // Two histograms merged
        assertEquals(360L, merged.getTotalFrequency()); // 185 + 175 = 360
    }

    @Test
    void testMergeThreeHistograms_correctSums() {
        StackHistogram h1 = createSampleHistogram1();
        StackHistogram h2 = createSampleHistogram2();
        StackHistogram h3 = createSampleHistogram3();

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2, h3));

        // Bucket 0: 100 + 80 + 120 = 300
        assertEquals(300L, merged.getFrequency(0));
        // Bucket 1: 50 + 60 + 40 = 150
        assertEquals(150L, merged.getFrequency(1));
        // Bucket 2: 25 + 0 + 0 = 25
        assertEquals(25L, merged.getFrequency(2));
        // Bucket 3: 0 + 30 + 0 = 30
        assertEquals(30L, merged.getFrequency(3));
        // Bucket 4: 0 + 0 + 20 = 20
        assertEquals(20L, merged.getFrequency(4));
        // Bucket 5: 10 + 5 + 0 = 15
        assertEquals(15L, merged.getFrequency(5));

        assertEquals(3, merged.getNumPartitions());
        assertEquals(540L, merged.getTotalFrequency()); // 185 + 175 + 180 = 540
    }

    @Test
    void testMergeInstanceMethod() {
        StackHistogram h1 = createSampleHistogram1();
        StackHistogram h2 = createSampleHistogram2();

        StackHistogram merged = h1.merge(h2);

        assertEquals(180L, merged.getFrequency(0));
        assertEquals(110L, merged.getFrequency(1));
        assertEquals(2, merged.getNumPartitions());
    }

    @Test
    void testMergeEmptyList_throwsException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> StackHistogram.merge(new ArrayList<>()),
                "Cannot merge empty list of histograms");
    }

    @Test
    void testMergeNullList_throwsException() {
        assertThrows(
                IllegalArgumentException.class,
                () -> StackHistogram.merge(null),
                "Cannot merge null list");
    }

    @Test
    void testMergeSingleHistogram_returnsSame() {
        StackHistogram h1 = createSampleHistogram1();
        StackHistogram merged = StackHistogram.merge(List.of(h1));

        assertEquals(h1.getHistogram(), merged.getHistogram());
        assertEquals(h1.getNumBuckets(), merged.getNumBuckets());
        assertEquals(h1.getMaxStackDistance(), merged.getMaxStackDistance());
        assertEquals(h1.getNumPartitions(), merged.getNumPartitions());
    }

    @Test
    void testMergeDifferentNumBuckets_throwsException() {
        StackHistogram h1 = createSampleHistogram1();
        StackHistogram h2 = createSampleHistogramDifferentBuckets();

        IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> StackHistogram.merge(List.of(h1, h2)),
                        "Should throw exception for different numBuckets");

        assertTrue(exception.getMessage().contains("different number of buckets"));
    }

    @Test
    void testMergeDifferentMaxStackDistance_usesMax() {
        // Note: According to Quickmrc, we take the max of maxStackDistance values
        StackHistogram h1 = createHistogram(Map.of(0, 100L), NUM_BUCKETS, 500L, 1);
        StackHistogram h2 = createHistogram(Map.of(0, 50L), NUM_BUCKETS, 1000L, 1);

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

        // Should use max of the two maxStackDistance values
        assertEquals(1000L, merged.getMaxStackDistance());
        assertEquals(150L, merged.getFrequency(0));
    }

    @Test
    void testMergeEmptyHistograms() {
        StackHistogram h1 = createHistogram(new HashMap<>());
        StackHistogram h2 = createHistogram(new HashMap<>());

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

        assertTrue(merged.getHistogram().isEmpty());
        assertEquals(0L, merged.getTotalFrequency());
        assertEquals(2, merged.getNumPartitions());
    }

    @Test
    void testMergeDisjointBuckets() {
        StackHistogram h1 = createHistogram(Map.of(0, 10L, 1, 20L));
        StackHistogram h2 = createHistogram(Map.of(5, 30L, 6, 40L));

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

        assertEquals(10L, merged.getFrequency(0));
        assertEquals(20L, merged.getFrequency(1));
        assertEquals(30L, merged.getFrequency(5));
        assertEquals(40L, merged.getFrequency(6));
        assertEquals(100L, merged.getTotalFrequency());
    }

    @Test
    void testMergeManyHistograms() {
        // Create 10 histograms, each with buckets 0-4 having frequencies 1-10
        List<StackHistogram> histograms = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            Map<Integer, Long> buckets = new HashMap<>();
            for (int b = 0; b < 5; b++) {
                buckets.put(b, (long) (i + 1));
            }
            histograms.add(createHistogram(buckets));
        }

        StackHistogram merged = StackHistogram.merge(histograms);

        // Sum 1+2+...+10 = 55 per bucket
        assertEquals(55L, merged.getFrequency(0));
        assertEquals(55L, merged.getFrequency(1));
        assertEquals(55L, merged.getFrequency(4));
        assertEquals(10, merged.getNumPartitions());
        assertEquals(275L, merged.getTotalFrequency()); // 55 * 5 buckets
    }

    @Test
    void testMergePreservesPartitionCount() {
        // Create histograms with different partition counts
        StackHistogram h1 = createHistogram(Map.of(0, 10L), NUM_BUCKETS, MAX_STACK_DISTANCE, 2);
        StackHistogram h2 = createHistogram(Map.of(0, 20L), NUM_BUCKETS, MAX_STACK_DISTANCE, 3);

        StackHistogram merged = StackHistogram.merge(List.of(h1, h2));

        // Partition count should be sum: 2 + 3 = 5
        assertEquals(5, merged.getNumPartitions());
        assertEquals(30L, merged.getFrequency(0));
    }

    @Test
    void testGetFrequency() {
        StackHistogram h = createSampleHistogram1();

        assertEquals(100L, h.getFrequency(0));
        assertEquals(50L, h.getFrequency(1));
        assertEquals(25L, h.getFrequency(2));
        assertEquals(0L, h.getFrequency(99)); // Non-existent bucket
    }

    @Test
    void testGetTotalFrequency() {
        StackHistogram h1 = createSampleHistogram1();
        assertEquals(185L, h1.getTotalFrequency()); // 100 + 50 + 25 + 10

        StackHistogram h2 = createSampleHistogram2();
        assertEquals(175L, h2.getTotalFrequency()); // 80 + 60 + 30 + 5
    }

    @Test
    void testEqualsAndHashCode() {
        Map<Integer, Long> buckets1 = Map.of(0, 100L, 1, 50L);
        Map<Integer, Long> buckets2 = Map.of(0, 100L, 1, 50L);

        StackHistogram h1 = createHistogram(buckets1);
        StackHistogram h2 = createHistogram(buckets2);

        assertEquals(h1, h2);
        assertEquals(h1.hashCode(), h2.hashCode());
    }

    @Test
    void testToString() {
        StackHistogram h = createSampleHistogram1();
        String str = h.toString();

        assertTrue(str.contains("StackHistogram"));
        assertTrue(str.contains("numBuckets=" + NUM_BUCKETS));
        assertTrue(str.contains("maxStackDistance=" + MAX_STACK_DISTANCE));
        assertTrue(str.contains("numPartitions=1"));
    }
}
