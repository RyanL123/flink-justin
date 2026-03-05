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

import org.apache.flink.autoscaler.utils.AutoScalerSerDeModule;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link MissRateCurve#leastMemoryMbForMissRate}. */
public class MissRateCurveTest {

    @Test
    void testLeastMemoryMbForMissRate_emptyPoints_throwsException() {
        MissRateCurve mrc = new MissRateCurve.Builder().build();

        assertThrows(IllegalStateException.class, () -> mrc.leastMemoryMbForMissRate(0.5));
    }

    @Test
    void testLeastMemoryMbForMissRate_singlePoint_exactMatch() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.3)
                .build();

        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.3);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(100.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_singlePoint_targetHigherThanMissRate() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.3)
                .build();

        // Target miss rate 0.5 is higher than the point's 0.3, so this point qualifies
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.5);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(100.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_singlePoint_targetLowerThanMissRate() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.5)
                .build();

        // Target miss rate 0.3 is lower than the point's 0.5, so no point qualifies
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.3);

        assertThat(result).isEmpty();
    }

    @Test
    void testLeastMemoryMbForMissRate_multiplePoints_findsSmallestQualifying() {
        // Miss rate decreases as cache size increases (typical behavior)
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(50.0, 0.8)   // Small cache, high miss rate
                .addPoint(100.0, 0.5)  // Medium cache, medium miss rate
                .addPoint(200.0, 0.2)  // Large cache, low miss rate
                .build();

        // With max miss rate 0.5, both 100MB (0.5) and 200MB (0.2) qualify
        // Should return 100MB as it's the smallest
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.5);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(100.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_multiplePoints_onlyLargestQualifies() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(50.0, 0.8)
                .addPoint(100.0, 0.5)
                .addPoint(200.0, 0.2)
                .build();

        // With max miss rate 0.25, only 200MB (0.2) qualifies
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.25);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(200.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_multiplePoints_noneQualify() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(50.0, 0.8)
                .addPoint(100.0, 0.5)
                .addPoint(200.0, 0.3)
                .build();

        // With max miss rate 0.1, no point qualifies (all have higher miss rates)
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.1);

        assertThat(result).isEmpty();
    }

    @Test
    void testLeastMemoryMbForMissRate_multiplePoints_allQualify() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(50.0, 0.3)
                .addPoint(100.0, 0.2)
                .addPoint(200.0, 0.1)
                .build();

        // With max miss rate 0.9, all points qualify
        // Should return 50MB as it's the smallest
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.9);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(50.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_unsortedInput_sortsCorrectly() {
        // Points added out of order - should still work correctly
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(200.0, 0.2)  // Added first but largest
                .addPoint(50.0, 0.8)   // Added second but smallest
                .addPoint(100.0, 0.5)  // Added third, middle
                .build();

        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.5);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(100.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_boundaryMaxMissRate_zero() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.0)  // Perfect cache - zero miss rate
                .addPoint(50.0, 0.5)
                .build();

        // Only the point with exactly 0.0 miss rate qualifies
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.0);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(100.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_boundaryMaxMissRate_one() {
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(50.0, 1.0)   // No cache - 100% miss rate
                .addPoint(100.0, 0.5)
                .build();

        // Both points qualify with max miss rate 1.0
        // Should return smallest (50.0)
        Optional<Double> result = mrc.leastMemoryMbForMissRate(1.0);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(50.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_sameMemoryDifferentMissRates() {
        // Edge case: multiple points with same cache size (shouldn't happen in practice)
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.3)
                .addPoint(100.0, 0.5)
                .build();

        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.4);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(100.0);
    }

    @Test
    void testLeastMemoryMbForMissRate_sameMissRateDifferentMemory() {
        // Two points with same miss rate but different memory
        MissRateCurve mrc = new MissRateCurve.Builder()
                .addPoint(100.0, 0.5)
                .addPoint(200.0, 0.5)
                .build();

        // Should return the smaller memory
        Optional<Double> result = mrc.leastMemoryMbForMissRate(0.5);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(100.0);
    }

    @Test
    void testJacksonRoundTrip() throws Exception {
        var objectMapper = new ObjectMapper();
        var mrc =
                new MissRateCurve(
                        List.of(
                                new MissRateCurve.MRCPoint(64.0, 0.6),
                                new MissRateCurve.MRCPoint(128.0, 0.3)));

        var restored = objectMapper.readValue(objectMapper.writeValueAsString(mrc), MissRateCurve.class);

        assertThat(restored.getPoints())
                .extracting(MissRateCurve.MRCPoint::getCacheSizeMb, MissRateCurve.MRCPoint::getMissRate)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(64.0, 0.6),
                        org.assertj.core.groups.Tuple.tuple(128.0, 0.3));
    }

    @Test
    void testJacksonRoundTripInVertexMap() throws Exception {
        var objectMapper = new ObjectMapper().registerModule(new AutoScalerSerDeModule());
        var vertexId = new JobVertexID();
        var curves =
                Map.of(
                        vertexId,
                        new MissRateCurve(
                                List.of(
                                        new MissRateCurve.MRCPoint(64.0, 0.6),
                                        new MissRateCurve.MRCPoint(128.0, 0.3))));

        var restored =
                objectMapper.readValue(
                        objectMapper.writeValueAsString(curves),
                        new TypeReference<Map<JobVertexID, MissRateCurve>>() {});

        assertThat(restored).containsKey(vertexId);
        assertThat(restored.get(vertexId).getPoints())
                .extracting(MissRateCurve.MRCPoint::getCacheSizeMb, MissRateCurve.MRCPoint::getMissRate)
                .containsExactly(
                        org.assertj.core.groups.Tuple.tuple(64.0, 0.6),
                        org.assertj.core.groups.Tuple.tuple(128.0, 0.3));
    }
}
