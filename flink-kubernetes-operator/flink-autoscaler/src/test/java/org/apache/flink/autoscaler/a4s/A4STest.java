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

import org.apache.flink.autoscaler.ScalingConfigurations;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.ShipStrategy;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link A4S}. */
public class A4STest {

    /** Base memory size in MB (same as in A4S class). */
    private static final double BASE_MEMORY_MB = 158.0;

    @Test
    void testMemoryMBToLevel_belowLevel0_roundsUpTo0() {
        // Memory below BASE rounds up to level 0
        assertThat(A4S.memoryMBToLevel(100.0)).isEqualTo(0);
        assertThat(A4S.memoryMBToLevel(50.0)).isEqualTo(0);
        assertThat(A4S.memoryMBToLevel(1.0)).isEqualTo(0);
    }

    @Test
    void testMemoryMBToLevel_slightlyAboveLevel0_roundsUpTo1() {
        // Memory slightly above level 0 (158 MB) rounds up to level 1
        assertThat(A4S.memoryMBToLevel(159.0)).isEqualTo(1);
        assertThat(A4S.memoryMBToLevel(200.0)).isEqualTo(1);
        assertThat(A4S.memoryMBToLevel(315.0)).isEqualTo(1);
    }

    @Test
    void testMemoryMBToLevel_slightlyAboveLevel1_roundsUpTo2() {
        // Memory slightly above level 1 (316 MB) rounds up to level 2
        assertThat(A4S.memoryMBToLevel(317.0)).isEqualTo(2);
        assertThat(A4S.memoryMBToLevel(400.0)).isEqualTo(2);
        assertThat(A4S.memoryMBToLevel(631.0)).isEqualTo(2);
    }

    @Test
    void testMemoryMBToLevel_aboveLevel2_cappedAtMaxLevel() {
        // Memory above level 2 is capped at MAX_MEMORY_LEVEL (2)
        assertThat(A4S.memoryMBToLevel(633.0)).isEqualTo(ScalingConfigurations.MAX_MEMORY_LEVEL);
        assertThat(A4S.memoryMBToLevel(900.0)).isEqualTo(ScalingConfigurations.MAX_MEMORY_LEVEL);
        assertThat(A4S.memoryMBToLevel(1264.0)).isEqualTo(ScalingConfigurations.MAX_MEMORY_LEVEL);
        assertThat(A4S.memoryMBToLevel(2000.0)).isEqualTo(ScalingConfigurations.MAX_MEMORY_LEVEL);
        assertThat(A4S.memoryMBToLevel(10000.0)).isEqualTo(ScalingConfigurations.MAX_MEMORY_LEVEL);
        assertThat(A4S.memoryMBToLevel(1000000.0)).isEqualTo(ScalingConfigurations.MAX_MEMORY_LEVEL);
    }

    @Test
    void testMemoryMBToLevel_zeroMemory() {
        // Zero memory should return level 0
        assertThat(A4S.memoryMBToLevel(0.0)).isEqualTo(0);
    }

    @Test
    void testMemoryMBToLevel_negativeMemory() {
        // Negative memory should return level 0
        assertThat(A4S.memoryMBToLevel(-1.0)).isEqualTo(0);
        assertThat(A4S.memoryMBToLevel(-100.0)).isEqualTo(0);
    }

    @Test
    void testMemoryMBToLevel_boundaryValues_roundUp() {
        // Test exact boundaries - values just above a level boundary round up
        // Just above level 0 boundary (158 MB) rounds up to level 1
        assertThat(A4S.memoryMBToLevel(BASE_MEMORY_MB + 0.001)).isEqualTo(1);

        // Just above level 1 boundary (316 MB) rounds up to level 2
        assertThat(A4S.memoryMBToLevel(2 * BASE_MEMORY_MB + 0.001)).isEqualTo(2);

        // Just above level 2 boundary (632 MB) would be level 3, but capped at 2
        assertThat(A4S.memoryMBToLevel(4 * BASE_MEMORY_MB + 0.001)).isEqualTo(2);
    }

    @Test
    void testMemoryMBToLevel_exactBoundaries() {
        // At exact boundaries, level equals the boundary level (no rounding needed)
        assertThat(A4S.memoryMBToLevel(BASE_MEMORY_MB)).isEqualTo(0);       // 158 MB -> level 0
        assertThat(A4S.memoryMBToLevel(2 * BASE_MEMORY_MB)).isEqualTo(1);   // 316 MB -> level 1
        assertThat(A4S.memoryMBToLevel(4 * BASE_MEMORY_MB)).isEqualTo(2);   // 632 MB -> level 2
        assertThat(A4S.memoryMBToLevel(8 * BASE_MEMORY_MB)).isEqualTo(2);   // 1264 MB -> capped at 2
    }

    // ==================== Tests for place method ====================

    @ParameterizedTest
    @CsvSource({
            "1, 800.0",
            "2, 500.0",
            "4, 300.0",
            "8, 200.0"
    })
    void testPlace_mpcWithMultipleParallelismLevels(int parallelism, double expectedMemory) {
        JobVertexID operator1 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        // MPC with multiple parallelism levels
        MemoryParallelismCurve mpc = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 800.0),
                new MemoryParallelismCurve.CurvePoint(2, 500.0),
                new MemoryParallelismCurve.CurvePoint(4, 300.0),
                new MemoryParallelismCurve.CurvePoint(8, 200.0)));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(operator1, parallelism);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(operator1, mpc);

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs);

        assertThat(result).isPresent();
        assertThat(result.get().get(operator1).getParallelism()).isEqualTo(parallelism);
        assertThat(result.get().get(operator1).getMemoryMB()).isEqualTo(expectedMemory);
    }

    @Test
    void testPlace_multipleOperatorsWithValidMPCs() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(2, 500.0),
                new MemoryParallelismCurve.CurvePoint(4, 300.0)));
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(3, 600.0),
                new MemoryParallelismCurve.CurvePoint(6, 400.0)));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(
                operator1, 2,
                operator2, 3);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs);

        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(2);
        assertThat(result.get().get(operator1).getParallelism()).isEqualTo(2);
        assertThat(result.get().get(operator1).getMemoryMB()).isEqualTo(500.0);
        assertThat(result.get().get(operator2).getParallelism()).isEqualTo(3);
        assertThat(result.get().get(operator2).getMemoryMB()).isEqualTo(600.0);
    }

    @Test
    void testPlace_operatorWithMissingMPC_skipped() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        // Only operator2 has an MPC, operator1 is not in the map
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(3, 600.0)));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(
                operator1, 2,
                operator2, 3);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(operator2, mpc2);

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs);

        // Result should be present, but operator1 is skipped
        assertThat(result).isPresent();
        assertThat(result.get()).hasSize(1);
        assertThat(result.get().containsKey(operator1)).isFalse();
        assertThat(result.get().get(operator2).getParallelism()).isEqualTo(3);
    }

    @Test
    void testPlace_oneOperatorFailsPlacement_returnsEmpty() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        // operator1's MPC supports parallelism 2, but operator2's MPC doesn't support parallelism 3
        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(2, 500.0)));
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(4, 400.0),
                new MemoryParallelismCurve.CurvePoint(6, 300.0)));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(
                operator1, 2,
                operator2, 3);  // parallelism 3 not in mpc2
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs);

        // Should return empty because operator2 cannot be placed
        assertThat(result).isEmpty();
    }

    @Test
    void testPlace_emptyTopology_returnsEmptyDecisions() {
        // Create an empty topology (no operators)
        JobTopology topology = new JobTopology();

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of();
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of();

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs);

        assertThat(result).isPresent();
        assertThat(result.get()).isEmpty();
    }

    @Test
    void testPlace_allOperatorsHaveNullMPC_returnsEmptyDecisions() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        Map<JobVertexID, Integer> parallelismForVertex = Map.of(
                operator1, 2,
                operator2, 3);
        // Empty MPC map - all operators have null/missing MPC
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of();

        Optional<Map<JobVertexID, A4S.Decision>> result = a4s.place(parallelismForVertex, mpcs);

        // All operators skipped, returns empty decisions
        assertThat(result).isPresent();
        assertThat(result.get()).isEmpty();
    }

    // ==================== Tests for increaseParallelism method ====================

    @Test
    void testIncreaseParallelism_singleOperator_returnsOperator() {
        JobVertexID operator1 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        // MPC: parallelism 1 -> 800 MB, parallelism 2 -> 500 MB (decrease of 300)
        MemoryParallelismCurve mpc = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 800.0),
                new MemoryParallelismCurve.CurvePoint(2, 500.0)));

        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 1);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(operator1, mpc);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(operator1);
    }

    @Test
    void testIncreaseParallelism_multipleOperators_returnsOperatorWithLargestMemoryDecrease() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        // operator1: parallelism 1 -> 800, parallelism 2 -> 700 (decrease of 100)
        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 800.0),
                new MemoryParallelismCurve.CurvePoint(2, 700.0)));

        // operator2: parallelism 1 -> 600, parallelism 2 -> 300 (decrease of 300 - larger)
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 600.0),
                new MemoryParallelismCurve.CurvePoint(2, 300.0)));

        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 1);
        parallelismForVertex.put(operator2, 1);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        // operator2 has the largest memory decrease (-300 vs -100)
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(operator2);
    }

    @Test
    void testIncreaseParallelism_allOperatorsAtMaxParallelism_returnsEmpty() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        // Both MPCs have max parallelism of 2
        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 800.0),
                new MemoryParallelismCurve.CurvePoint(2, 500.0)));
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 600.0),
                new MemoryParallelismCurve.CurvePoint(2, 300.0)));

        // Both operators at max parallelism
        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 2);
        parallelismForVertex.put(operator2, 2);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        assertThat(result).isEmpty();
    }

    @Test
    void testIncreaseParallelism_oneOperatorAtMaxParallelism_returnsOther() {
        JobVertexID operator1 = new JobVertexID();
        JobVertexID operator2 = new JobVertexID();
        JobTopology topology = new JobTopology(
                new VertexInfo(operator1, Map.of(), 1, 100),
                new VertexInfo(operator2, Map.of(operator1, ShipStrategy.REBALANCE), 1, 100));

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        // operator1 has max parallelism of 2
        MemoryParallelismCurve mpc1 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 800.0),
                new MemoryParallelismCurve.CurvePoint(2, 500.0)));

        // operator2 has max parallelism of 4
        MemoryParallelismCurve mpc2 = new MemoryParallelismCurve(1000.0, List.of(
                new MemoryParallelismCurve.CurvePoint(1, 600.0),
                new MemoryParallelismCurve.CurvePoint(2, 400.0),
                new MemoryParallelismCurve.CurvePoint(3, 350.0),
                new MemoryParallelismCurve.CurvePoint(4, 300.0)));

        // operator1 at max, operator2 can still increase
        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        parallelismForVertex.put(operator1, 2);
        parallelismForVertex.put(operator2, 2);
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of(
                operator1, mpc1,
                operator2, mpc2);

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        // Only operator2 can increase parallelism
        assertThat(result).isPresent();
        assertThat(result.get()).isEqualTo(operator2);
    }

    @Test
    void testIncreaseParallelism_emptyTopology_returnsEmpty() {
        JobTopology topology = new JobTopology();

        A4S a4s = new A4S(topology, new EvaluatedMetrics(Map.of(), Map.of()));

        Map<JobVertexID, Integer> parallelismForVertex = new HashMap<>();
        Map<JobVertexID, MemoryParallelismCurve> mpcs = Map.of();

        Optional<JobVertexID> result = a4s.increaseParallelism(parallelismForVertex, mpcs);

        assertThat(result).isEmpty();
    }
}
