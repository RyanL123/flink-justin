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

import org.junit.jupiter.api.Test;

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

}
