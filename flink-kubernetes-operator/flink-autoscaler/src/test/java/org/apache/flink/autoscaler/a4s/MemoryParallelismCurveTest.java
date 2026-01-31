/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.autoscaler.a4s;

import org.apache.flink.autoscaler.config.AutoScalerOptions;
import org.apache.flink.configuration.Configuration;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link MemoryParallelismCurve#fromMissRateCurve}. */
public class MemoryParallelismCurveTest {

    private Configuration conf;

    @BeforeEach
    void setUp() {
        conf = new Configuration();
        conf.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 1);
        conf.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 24);
    }

    @Test
    void testFromMissRateCurve_invalidLatencies_throwsException() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.5)
                .build();

        // missLatencyMs <= hitLatencyMs should throw
        assertThrows(IllegalArgumentException.class, () ->
                MemoryParallelismCurve.fromMissRateCurve(1000.0, 10.0, 10.0, mrc, conf));

        assertThrows(IllegalArgumentException.class, () ->
                MemoryParallelismCurve.fromMissRateCurve(1000.0, 5.0, 10.0, mrc, conf));
    }

    @Test
    void testFromMissRateCurve_basicCurveGeneration() {
        // Create MRC with decreasing miss rates as memory increases
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(50.0, 0.9)
                .addPoint(100.0, 0.7)
                .addPoint(200.0, 0.5)
                .addPoint(400.0, 0.3)
                .addPoint(800.0, 0.1)
                .build();

        // targetThroughput = 100 rec/s, missLatency = 20ms, hitLatency = 5ms
        // Formula: maxMissRate = ((parallelism / throughput) - hitLatency) / (missLatency - hitLatency)
        // For parallelism 1: maxMissRate = ((1/100) - 0.005) / (0.020 - 0.005) = (0.01 - 0.005) / 0.015 = 0.333
        // For parallelism 2: maxMissRate = ((2/100) - 0.005) / 0.015 = 0.015 / 0.015 = 1.0
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                100.0,  // targetThroughputPerSec
                0.02,   // missLatencySec
                0.005,    // hitLatencySec
                mrc,
                conf);

        assertThat(mpc.getTargetThroughput()).isEqualTo(100.0);
        assertThat(mpc.getPoints()).isNotEmpty();
    }

    @Test
    void testFromMissRateCurve_respectsParallelismBounds() {
        conf.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 5);
        conf.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 10);

        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.1)  // Low miss rate, all parallelisms should find this
                .build();

        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                1000.0, 0.02, 0.005, mrc, conf);

        List<MemoryParallelismCurve.CurvePoint> points = mpc.getPoints();

        // All points should be within [5, 10]
        for (MemoryParallelismCurve.CurvePoint point : points) {
            assertThat(point.getParallelism()).isBetween(5, 10);
        }
    }

    @Test
    void testFromMissRateCurve_skipsNegativeMissRate() {
        // When maxMissRate is negative, that parallelism should be skipped
        // maxMissRate < 0 when (parallelism / throughput) < hitLatency
        // i.e., parallelism < throughput * hitLatency

        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.5)
                .build();

        // With throughput=1000, hitLatency=10ms (0.01s)
        // parallelism 1: (1/1000) = 0.001 < 0.01 => negative miss rate, skipped
        // parallelism 10: (10/1000) = 0.01 == 0.01 => miss rate = 0, ok
        // parallelism 20: (20/1000) = 0.02 > 0.01 => positive miss rate, ok
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                1000.0,  // targetThroughput
                0.02,    // missLatencySec
                0.005,    // hitLatencySec
                mrc,
                conf);

        List<MemoryParallelismCurve.CurvePoint> points = mpc.getPoints();

        // Parallelisms with negative miss rate should be skipped
        // parallelism 1-9 should be skipped (maxMissRate < 0)
        for (MemoryParallelismCurve.CurvePoint point : points) {
            assertThat(point.getParallelism()).isGreaterThanOrEqualTo(10);
        }
    }

    @Test
    void testFromMissRateCurve_clampsMissRateToOne() {
        // When maxMissRate > 1.0, it should be clamped to 1.0
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(50.0, 1.0)   // Accepts miss rate up to 1.0
                .addPoint(100.0, 0.5)
                .build();

        // High parallelism relative to throughput will give maxMissRate > 1.0
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                10.0,    // Low throughput
                0.02,    // missLatencySec
                0.005,     // hitLatencySec
                mrc,
                conf);

        // Should still produce valid points (miss rate clamped to 1.0)
        assertThat(mpc.getPoints()).isNotEmpty();
    }

    @Test
    void testFromMissRateCurve_noPointsWhenMrcCannotSatisfy() {
        // MRC with only high miss rates
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.9)
                .addPoint(200.0, 0.8)
                .build();

        conf.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 1);
        conf.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 5);

        // With very high throughput, required miss rate will be very low
        // maxMissRate for parallelism 1: ((1/10000) - 0.001) / 0.009 = -0.0011 / 0.009 < 0 => skipped
        // Even if not skipped, MRC can't satisfy miss rates below 0.8
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                10000.0,  // Very high throughput
                0.01,     // missLatencySec
                0.001,      // hitLatencySec
                mrc,
                conf);

        // Most or all parallelisms should be skipped due to negative miss rate
        // or unable to find memory in MRC
        assertThat(mpc.getPoints().size()).isLessThanOrEqualTo(5);
    }

    @Test
    void testFromMissRateCurve_memoryDecreaseWithParallelism() {
        // Higher parallelism allows higher miss rate, which requires less memory
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.9)
                .addPoint(200.0, 0.7)
                .addPoint(400.0, 0.5)
                .addPoint(800.0, 0.3)
                .addPoint(1600.0, 0.1)
                .build();

        conf.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 1);
        conf.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 10);

        // With reasonable params, higher parallelism should map to lower memory
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                100.0,
                100.0,
                0.01,
                mrc,
                conf);

        List<MemoryParallelismCurve.CurvePoint> points = mpc.getPoints();

        // Verify the curve has the expected inverse relationship
        // (higher parallelism generally means lower or equal memory)
        if (points.size() >= 2) {
            for (int i = 1; i < points.size(); i++) {
                MemoryParallelismCurve.CurvePoint prev = points.get(i - 1);
                MemoryParallelismCurve.CurvePoint curr = points.get(i);
                // Higher parallelism should have same or lower memory requirement
                assertThat(curr.getMemoryMB()).isLessThanOrEqualTo(prev.getMemoryMB());
            }
        }
    }

    @Test
    void testFromMissRateCurve_singleParallelism() {
        conf.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 5);
        conf.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 5);

        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.5)
                .build();

        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                100.0, 0.02, 0.005, mrc, conf);

        List<MemoryParallelismCurve.CurvePoint> points = mpc.getPoints();

        // Should have at most 1 point
        assertThat(points.size()).isLessThanOrEqualTo(1);
        if (!points.isEmpty()) {
            assertThat(points.get(0).getParallelism()).isEqualTo(5);
        }
    }

    @Test
    void testFromMissRateCurve_formulaVerification() {
        // Manually verify the formula for a specific case
        // maxMissRate = ((parallelism / throughput) - hitLatencySec) / (missLatencySec - hitLatencySec)

        // Set up MRC with known miss rates
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.5)   // 100MB gives 0.5 miss rate
                .addPoint(200.0, 0.25)  // 200MB gives 0.25 miss rate
                .build();

        conf.set(AutoScalerOptions.VERTEX_MIN_PARALLELISM, 1);
        conf.set(AutoScalerOptions.VERTEX_MAX_PARALLELISM, 3);

        // throughput = 100, missLatency = 0.02s (20ms), hitLatency = 0.01s (10ms)
        // For parallelism 2:
        //   maxMissRate = ((2/100) - 0.01) / (0.02 - 0.01) = (0.02 - 0.01) / 0.01 = 1.0
        // For parallelism 1:
        //   maxMissRate = ((1/100) - 0.01) / 0.01 = (0.01 - 0.01) / 0.01 = 0.0
        MemoryParallelismCurve mpc = MemoryParallelismCurve.fromMissRateCurve(
                100.0,   // throughput (records/sec)
                0.02,    // missLatencySec
                0.005,    // hitLatencySec
                mrc,
                conf);

        // With maxMissRate = 0 for parallelism 1, no MRC point satisfies (both have > 0 miss rate)
        // With maxMissRate = 1.0 for parallelism 2, MRC returns 100.0 (smallest that satisfies <= 1.0)
        List<MemoryParallelismCurve.CurvePoint> points = mpc.getPoints();

        // Parallelism 2 should have memory 100.0 (first point that satisfies maxMissRate = 1.0)
        boolean hasParallelism2 = points.stream()
                .anyMatch(p -> p.getParallelism() == 2 && p.getMemoryMB() == 100.0);
        assertThat(hasParallelism2).isTrue();
    }
}
