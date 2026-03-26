package org.apache.flink.runtime.a4s.core;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MissRateCurveTest {
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
}
