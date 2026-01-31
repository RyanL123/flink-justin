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

import lombok.Getter;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.autoscaler.ScalingConfigurations.ScalingInformation;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.autoscaler.topology.VertexInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.*;

/**
 * A4S Scaling Policy implementation.
 *
 * <p>This class implements the co-designed scaling and placement strategy from the A4S paper.
 * The key insight is that for stateful operators, memory and parallelism are correlated -
 * we can trade one for the other while maintaining target throughput.
 *
 * <p>The algorithm works by:
 * 1. Generating/estimating a memory-parallelism curve for the target throughput
 * 2. Finding valid (memory, parallelism) configurations within resource constraints
 * 3. Selecting the optimal configuration that minimizes total resource usage
 * 4. Mapping the continuous memory value to the closest discrete memory level
 */
public class A4SScalingPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(A4SScalingPolicy.class);

    /** Discrete memory levels available (in relative units). */
    public static final int[] MEMORY_LEVELS = {0, 1, 2, 3};

    /** Base memory size in MB for level 0. */
    private static final double BASE_MEMORY_MB = 158.0;

    /**
     * Result of the A4S scaling decision.
     */
    public static class A4SDecision {
        @Getter
        private final int parallelism;
        
        @Getter
        private final int memoryLevel;
        
        @Getter
        private final double estimatedMemoryMB;
        
        @Getter
        private final String reason;

        public A4SDecision(int parallelism, int memoryLevel, double estimatedMemoryMB,
                          String reason) {
            this.parallelism = parallelism;
            this.memoryLevel = memoryLevel;
            this.estimatedMemoryMB = estimatedMemoryMB;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return String.format("A4SDecision[p=%d, memLevel=%d, estMem=%.2fMB, reason=%s]",
                    parallelism, memoryLevel, estimatedMemoryMB, reason);
        }
    }

    /**
     * Make a scaling decision using the A4S algorithm.
     *
     * @param currentInfo current scaling information for the operator
     * @param targetThroughput target throughput for the operator
     * @param conf configuration options
     * @return the A4S scaling decision
     */
    public A4SDecision makeDecision(
            ScalingInformation currentInfo,
            double targetThroughput,
            Configuration conf) {
        // stateless operator
        if (currentInfo.getAvgCacheHitRate() == 0.0) {
            LOG.info("A4S: Stateless operator detected, no scaling needed");
            return new A4SDecision(currentInfo.getParallelism(), -1, 0, 
                    "Stateless operator - using proposed parallelism");
        }

        MemoryParallelismCurve mpc = estimateMemoryParallelismCurve(currentInfo, targetThroughput, conf);
        LOG.info("A4S: Estimated memory-parallelism curve: {}", mpc);

        return selectOptimalFromCurve(mpc, conf);
        
    }

    /**
     * Select the optimal configuration from the memory-parallelism curve.
     */
    private A4SDecision selectOptimalFromCurve(
            MemoryParallelismCurve mpc,
            Configuration conf) {

        int maxParallelism = conf.get(VERTEX_MAX_PARALLELISM);
        int maxMemoryLevel = conf.get(A4S_MAX_MEMORY_LEVEL);
        double minMemoryMB = getMemoryForLevel(0);
        double maxMemoryMB = getMemoryForLevel(maxMemoryLevel);

        Optional<MemoryParallelismCurve.CurvePoint> optimal = mpc.findOptimalConfiguration(
                1, maxParallelism, minMemoryMB, maxMemoryMB);

        if (optimal.isEmpty()) {
            LOG.warn("A4S: No optimal configuration found, using fallback");
            return new A4SDecision(maxParallelism, 0, maxMemoryMB,
                    "Fallback - no optimal configuration found on curve");
        }

        MemoryParallelismCurve.CurvePoint point = optimal.get();
        int memoryLevel = memoryMBToLevel(point.getMemoryMB());
        
        LOG.info("A4S: Selected optimal configuration from curve: parallelism={}, memoryLevel={}", 
                point.getParallelism(), memoryLevel);
        
        return new A4SDecision(point.getParallelism(), memoryLevel,
                point.getMemoryMB(), "Optimal configuration found on curve");
    }

    /**
     * Estimate a memory-parallelism curve based on current metrics.
     *
     * <p>The estimation uses the following approach:
     * <ol>
     *   <li>We have the current parallelism, memory level, and throughput as a reference point</li>
     *   <li>Assume linear relationship between throughput ratio and parallelism scaling</li>
     *   <li>Assume inverse relationship: as parallelism increases, memory level decreases</li>
     * </ol>
     *
     * <p>The base parallelism for target throughput is calculated using linear scaling:
     * baseParallelism = currentParallelism * (targetThroughput / currentThroughput)
     *
     * <p>Then for each memory level, we adjust the parallelism based on the memory level
     * difference from the current level. Higher memory levels allow lower parallelism
     * (more efficient per-task processing due to better cache utilization).
     */
    @VisibleForTesting
    public static MemoryParallelismCurve estimateMemoryParallelismCurve(
            ScalingInformation info,
            double targetThroughput,
            Configuration conf) {

        LOG.info("A4S: Estimating memory-parallelism curve for target throughput {}", targetThroughput);

        int currentParallelism = info.getParallelism();
        int currentMemoryLevel = info.getMemoryLevel();
        double currentThroughput = info.getAvgThroughput();

        // Linear scaling: base parallelism needed to achieve target throughput
        // at the current memory level
        double throughputRatio = targetThroughput / currentThroughput;
        double baseParallelism = currentParallelism * throughputRatio;

        LOG.info("A4S: Current state - parallelism={}, memoryLevel={}, throughput={}",
                currentParallelism, currentMemoryLevel, currentThroughput);
        LOG.info("A4S: Throughput ratio={}, base parallelism for target={}", 
                throughputRatio, baseParallelism);

        MemoryParallelismCurve.Builder builder = MemoryParallelismCurve.builder()
                .targetThroughput(targetThroughput);

        int maxPar = conf.get(VERTEX_MAX_PARALLELISM);
        int maxMemLevel = conf.get(A4S_MAX_MEMORY_LEVEL);

        // Memory scaling factor: each memory level change affects parallelism
        // Higher memory = more efficient tasks = less parallelism needed
        // Lower memory = less efficient tasks = more parallelism needed
        double memoryScaleFactor = conf.get(A4S_MEMORY_PARALLELISM_SCALE_FACTOR);

        for (int memLevel = 0; memLevel <= maxMemLevel; memLevel++) {
            double memoryMB = getMemoryForLevel(memLevel);

            // Calculate the memory level difference from current
            int memLevelDiff = memLevel - currentMemoryLevel;

            // Adjust parallelism based on memory level difference
            // Higher memory (positive diff) -> multiply by factor < 1 -> less parallelism
            // Lower memory (negative diff) -> multiply by factor > 1 -> more parallelism
            double adjustedParallelism = baseParallelism * Math.pow(memoryScaleFactor, -memLevelDiff);

            int requiredParallelism = (int) Math.ceil(adjustedParallelism);
            requiredParallelism = Math.max(1, Math.min(maxPar, requiredParallelism));

            LOG.debug("A4S: memLevel={}, memLevelDiff={}, adjustedPar={}, requiredPar={}",
                    memLevel, memLevelDiff, adjustedParallelism, requiredParallelism);

            builder.addPoint(requiredParallelism, memoryMB);
        }

        return builder.build();
    }

    /**
     * Convert memory in MB to the closest discrete memory level.
     */
    @VisibleForTesting
    public static int memoryMBToLevel(double memoryMB) {
        // Memory levels are: 0 -> BASE, 1 -> 2*BASE, 2 -> 4*BASE, etc.
        double ratio = memoryMB / BASE_MEMORY_MB;
        
        int level = 0;
        double threshold = 1.0;
        
        for (int l : MEMORY_LEVELS) {
            double nextThreshold = Math.pow(2, l + 1);
            if (ratio >= threshold && ratio < nextThreshold) {
                level = l;
                break;
            }
            threshold = nextThreshold;
            level = l;
        }
        
        return Math.min(level, MEMORY_LEVELS[MEMORY_LEVELS.length - 1]);
    }

    /**
     * Get the memory in MB for a given memory level.
     */
    @VisibleForTesting
    static double getMemoryForLevel(int level) {
        if (level < 0) {
            return 0;
        }
        return BASE_MEMORY_MB * Math.pow(2, level);
    }

    /**
     * Apply the A4S decision to a ScalingInformation object.
     */
    public void applyDecision(ScalingInformation info, A4SDecision decision) {
        info.setParallelism(decision.getParallelism());
        info.setMemoryLevel(decision.getMemoryLevel());
        
        LOG.info("A4S: Applied decision to scaling info: {}", decision);
    }
}
