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
import org.apache.flink.autoscaler.ScalingConfigurations;
import org.apache.flink.autoscaler.metrics.EvaluatedMetrics;
import org.apache.flink.autoscaler.topology.JobTopology;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.autoscaler.config.AutoScalerOptions.*;

/**
 * A4S Scaling Policy implementation.
 */
public class A4S {

    private static final Logger LOG = LoggerFactory.getLogger(A4S.class);

    /** Base memory size in MB for level 0. */
    private static final double BASE_MEMORY_MB = 158.0;

    private final EvaluatedMetrics evaluatedMetrics;

    private final List<JobVertexID> operators;

    public A4S(JobTopology jobTopology, EvaluatedMetrics evaluatedMetrics) {
        this.evaluatedMetrics = evaluatedMetrics;
        this.operators = jobTopology.getVerticesInTopologicalOrder();
    }

    public static class Decision {
        @Getter
        private final int parallelism;
        
        @Getter
        private final double memoryMB;
        

        public Decision(int parallelism, double memoryMB) {
            this.parallelism = parallelism;
            this.memoryMB = memoryMB;
        }

        @Override
        public String toString() {
            return String.format("A4SDecision[p=%d, mem=%.2fMB]",
                    parallelism, memoryMB);
        }
    }

    /**
     * Make a scaling decision using the A4S algorithm.
     */
    public Map<JobVertexID, Decision> makeDecision(Configuration conf) {
        int minParallelism = conf.get(VERTEX_MIN_PARALLELISM);

        Map<JobVertexID, MemoryParallelismCurve> mpcs = this.evaluatedMetrics.getMemoryParallelismCurves();
        Map<JobVertexID, Integer> parallelismForVertex = this.operators.
            stream().collect(Collectors.toMap(
                operator -> operator,
                operator -> Optional.ofNullable(mpcs.get(operator)).map(MemoryParallelismCurve::getMinParallelism).orElse(minParallelism)
        ));
        
        int maxAttempts = 10;
        for (int attempt = 0; attempt < maxAttempts; attempt++) {
            LOG.info("A4S: Attempt {} - Making decisions", attempt);
            Optional<Map<JobVertexID, Decision>> decisions = place(parallelismForVertex, mpcs);
            if (decisions.isPresent()) {
                LOG.info("A4S: Decisions: {}", decisions.get());
                return decisions.get();
            }

            LOG.info("A4S: No decision can be made, increasing parallelism");
            Optional<JobVertexID> operator = increaseParallelism(parallelismForVertex, mpcs);
            if (operator.isPresent()) {
                int newParallelism = parallelismForVertex.get(operator.get()) + 1;
                parallelismForVertex.put(operator.get(), newParallelism);
                LOG.info("A4S: Increasing parallelism for operator {} to {}", operator.get(), newParallelism);
            } else {
                LOG.warn("A4S: No operator can increase parallelism");
                break;
            }
        }

        LOG.warn("A4S: No decision can be made for any operator");
        return Map.of();
    }

    @VisibleForTesting
    Optional<Map<JobVertexID, Decision>> place(
        Map<JobVertexID, Integer> parallelismForVertex,
        Map<JobVertexID, MemoryParallelismCurve> memoryParallelismCurves) {
        Map<JobVertexID, Decision> decisions = new HashMap<>();

        for (JobVertexID operator : operators) {
            MemoryParallelismCurve mpc = memoryParallelismCurves.get(operator);
            int parallelism = parallelismForVertex.get(operator);

            if (mpc == null) {
                LOG.warn("A4S: No memory parallelism curve found for operator {}", operator);
                continue;
            }

            Optional<Double> memoryOpt = mpc.getMemoryMbForParallelism(parallelism);
            if (memoryOpt.isEmpty()) {
                LOG.warn("A4S: No memory point found for operator {} with parallelism {}, vertex cannot be scaled", operator, parallelism);
                return Optional.empty();
            }

            Decision decision = new Decision(parallelism, memoryOpt.get());
            decisions.put(operator, decision);
        }

        return Optional.of(decisions);
    }

    /**
     * Find an operator whose memory needs decrease most when its parallelism is increased by 1.
     */
    @VisibleForTesting
    Optional<JobVertexID> increaseParallelism(
        Map<JobVertexID, Integer> parallelismForVertex,
        Map<JobVertexID, MemoryParallelismCurve> mpcs) {

        double minMemoryDiff = Double.MAX_VALUE;
        JobVertexID minMemoryDiffOperator = null;

        for (JobVertexID operator : operators) {
            int parallelism = parallelismForVertex.get(operator);
            MemoryParallelismCurve mpc = mpcs.get(operator);

            if (parallelism + 1 > mpc.getMaxParallelism()) {
                continue;
            }
            double memoryDiff = mpc.getMemoryMbForParallelism(parallelism + 1).get() - 
                mpc.getMemoryMbForParallelism(parallelism).get();
            if (memoryDiff < minMemoryDiff) {
                minMemoryDiff = memoryDiff;
                minMemoryDiffOperator = operator;
            }
        }

        return Optional.ofNullable(minMemoryDiffOperator);
    }

    /**
     * Convert memory in MB to the nearest discrete memory level, always rounding up.
     *
     * <p>The level is capped by {@link ScalingConfigurations#MAX_MEMORY_LEVEL}.
     * Memory values above MAX_MEMORY_LEVEL are rounded down to MAX_MEMORY_LEVEL.
     *
     * @param memoryMB the memory in MB to convert
     * @return the memory level (0 to MAX_MEMORY_LEVEL)
     */
    public static int memoryMBToLevel(double memoryMB) {
        if (memoryMB <= 0) {
            return 0;
        }
        double ratio = memoryMB / BASE_MEMORY_MB;

        // Calculate log base 2 and round up to nearest level
        int level = (int) Math.ceil(Math.log(ratio) / Math.log(2));
        level = Math.max(0, level);
        return Math.min(level, ScalingConfigurations.MAX_MEMORY_LEVEL);
    }
}
