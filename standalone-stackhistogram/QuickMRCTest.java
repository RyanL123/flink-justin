import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class QuickMRCTest {

    private static final int NUM_BUCKETS = 50;
    private static final long MAX_STACK_DISTANCE = 500L;

    // Helper to create simple deterministic histograms
    private StackHistogram createSimpleHistogram(Map<Integer, Long> buckets, int partitions) {
        return new StackHistogram(buckets, NUM_BUCKETS, MAX_STACK_DISTANCE, partitions);
    }

    /**
     * Test 1: Verify Merge Logic
     * Ensures bucket counts and partition counts are summed correctly.
     */
    @Test
    void testMerge_preservesSumsAndPartitions() {
        Random random = new Random(42);
        List<StackHistogram> histograms = new ArrayList<>();
        Map<Integer, Long> expectedBuckets = new HashMap<>();
        int expectedPartitions = 0;

        for (int i = 0; i < 5; i++) {
            int partitions = 1 + random.nextInt(3);
            Map<Integer, Long> buckets = new HashMap<>();
            buckets.put(random.nextInt(NUM_BUCKETS), (long) random.nextInt(100));
            
            StackHistogram h = createSimpleHistogram(buckets, partitions);
            histograms.add(h);

            // Track expected totals
            buckets.forEach((k, v) -> expectedBuckets.merge(k, v, Long::sum));
            expectedPartitions += partitions;
        }

        StackHistogram merged = StackHistogram.merge(histograms);

        assertEquals(expectedPartitions, merged.getNumPartitions());
        for (Map.Entry<Integer, Long> e : expectedBuckets.entrySet()) {
            assertEquals(e.getValue(), merged.getFrequency(e.getKey()));
        }
    }

    /**
     * Test 2: Verify Unscaled MRC Math
     * Checks CDF logic: Miss Rate = 1.0 - CumulativeHitRate.
     */
    @Test
    void testUnscaledMRC_mathCorrectness() {
        // Bucket 0 has 100 items. Total = 100.
        // This means for any cache size covering bucket 0, Hit Rate should be 1.0, Miss Rate 0.0.
        // For cache size 0 (before bucket 0), Hit Rate is 0.0, Miss Rate 1.0.
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 100L);
        StackHistogram histogram = createSimpleHistogram(buckets, 1);

        List<QuickMRC.MRCPoint> mrc = QuickMRC.computeUnscaledMRC(histogram);

        // Check start (Cache Size 0 -> Miss Rate 1.0)
        assertEquals(0L, mrc.get(0).getCacheSize());
        assertEquals(1.0, mrc.get(0).getMissRate(), 0.001);

        // Check end (Full Cache -> Miss Rate 0.0)
        QuickMRC.MRCPoint lastPoint = mrc.get(mrc.size() - 1);
        assertEquals(0.0, lastPoint.getMissRate(), 0.001);
    }

    /**
     * Test 3: Verify Horizontal Scaling Logic
     * Explicitly checks that Cache Size is multiplied by the factor.
     */
    @Test
    void testScaledMRC_horizontalScalingLogic() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 100L); // 100 hits in first bucket
        StackHistogram histogram = createSimpleHistogram(buckets, 1);

        long scaleFactor = 4; // e.g. 4 partitions
        List<QuickMRC.MRCPoint> scaled = QuickMRC.computeScaledMRC(histogram, scaleFactor, 0);
        List<QuickMRC.MRCPoint> unscaled = QuickMRC.computeUnscaledMRC(histogram);

        assertEquals(unscaled.size(), scaled.size());

        for (int i = 0; i < scaled.size(); i++) {
            long expectedSize = unscaled.get(i).getCacheSize() * scaleFactor;
            assertEquals(expectedSize, scaled.get(i).getCacheSize(), "X-Axis should be scaled");
            assertEquals(unscaled.get(i).getMissRate(), scaled.get(i).getMissRate(), 0.0001, "Y-Axis should be unchanged");
        }
    }

    /**
     * Test 4: Verify Default Scaling
     * Ensures the method defaults to using the histogram's partition count.
     */
    @Test
    void testScaledMRC_defaultsToPartitionCount() {
        Map<Integer, Long> buckets = new HashMap<>();
        buckets.put(0, 10L);
        int partitions = 8;
        StackHistogram histogram = createSimpleHistogram(buckets, partitions);

        List<QuickMRC.MRCPoint> scaled = QuickMRC.computeScaledMRC(histogram);
        List<QuickMRC.MRCPoint> unscaled = QuickMRC.computeUnscaledMRC(histogram);

        // First non-zero point should be scaled by 8
        long unscaledX = unscaled.get(1).getCacheSize();
        long scaledX = scaled.get(1).getCacheSize();
        
        assertEquals(unscaledX * partitions, scaledX);
    }

    /**
     * Test 5: Edge Case
     * Empty histogram should not crash.
     */
    @Test
    void testMRC_emptyHistogram() {
        StackHistogram empty = createSimpleHistogram(new HashMap<>(), 1);
        assertTrue(QuickMRC.computeUnscaledMRC(empty).isEmpty());
        assertTrue(QuickMRC.computeScaledMRC(empty).isEmpty());
    }

    /**
     * Test 6: Full End-to-End Pipeline
     * Random Histograms -> Merged -> Unscaled MRC -> Scaled MRC
     */
    @Test
    void testEndToEndPipeline() {
        // 1. Setup: Create 3 histograms simulating 3 different partitions
        // Total Partitions = 1 + 1 + 1 = 3
        StackHistogram h1 = createSimpleHistogram(Map.of(0, 10L, 1, 20L), 1);
        StackHistogram h2 = createSimpleHistogram(Map.of(0, 15L, 1, 25L), 1);
        StackHistogram h3 = createSimpleHistogram(Map.of(0, 5L,  1, 5L),  1);

        List<StackHistogram> rawHistograms = Arrays.asList(h1, h2, h3);

        // 2. Merge Phase
        StackHistogram merged = StackHistogram.merge(rawHistograms);

        // Verify Merged State
        assertEquals(3, merged.getNumPartitions()); // 1+1+1
        assertEquals(30L, merged.getFrequency(0));  // 10+15+5
        assertEquals(50L, merged.getFrequency(1));  // 20+25+5
        long totalFreq = 30L + 50L; // 80 total accesses

        // 3. Unscaled MRC Phase
        List<QuickMRC.MRCPoint> unscaled = QuickMRC.computeUnscaledMRC(merged);

        // Verify specific point on Unscaled Curve
        // Bucket 0 covers range [0, width).
        // At cache size = width (end of bucket 0), we should have hit all items in bucket 0.
        // Hits = 30. Misses = Total - 30 = 50.
        // Miss Rate = 50 / 80 = 0.625
        QuickMRC.MRCPoint pointAfterBucket0 = unscaled.get(1); // Index 1 is usually end of bucket 0
        assertEquals(0.625, pointAfterBucket0.getMissRate(), 0.0001);

        // 4. Scaled MRC Phase
        List<QuickMRC.MRCPoint> scaled = QuickMRC.computeScaledMRC(merged);

        // Verify Scaling Relationship
        // The Scaled X value should be Unscaled X * TotalPartitions (3)
        // The Scaled Y value should be exactly the same as Unscaled Y
        assertEquals(unscaled.size(), scaled.size());
        
        for (int i = 0; i < unscaled.size(); i++) {
            QuickMRC.MRCPoint u = unscaled.get(i);
            QuickMRC.MRCPoint s = scaled.get(i);

            assertEquals(u.getCacheSize() * 3, s.getCacheSize(), "X-Axis must be scaled by partition count");
            assertEquals(u.getMissRate(), s.getMissRate(), 0.0001, "Y-Axis (Miss Rate) must remain unchanged");
        }
    }
}