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
import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        private final boolean isVerticalScaling;
        
        @Getter
        private final boolean isHorizontalScaling;
        
        @Getter
        private final String reason;

        public A4SDecision(int parallelism, int memoryLevel, double estimatedMemoryMB,
                          boolean isVerticalScaling, boolean isHorizontalScaling, String reason) {
            this.parallelism = parallelism;
            this.memoryLevel = memoryLevel;
            this.estimatedMemoryMB = estimatedMemoryMB;
            this.isVerticalScaling = isVerticalScaling;
            this.isHorizontalScaling = isHorizontalScaling;
            this.reason = reason;
        }

        @Override
        public String toString() {
            return String.format("A4SDecision[p=%d, memLevel=%d, estMem=%.2fMB, vScale=%b, hScale=%b, reason=%s]",
                    parallelism, memoryLevel, estimatedMemoryMB, isVerticalScaling, isHorizontalScaling, reason);
        }
    }

    /**
     * Make a scaling decision using the A4S algorithm.
     *
     * @param currentInfo current scaling information for the operator
     * @param previousInfo previous scaling information (may be null)
     * @param proposedParallelism the parallelism proposed by the base autoscaler
     * @param targetThroughput target throughput to achieve
     * @param conf configuration options
     * @return the A4S scaling decision
     */
    public A4SDecision makeDecision(
            ScalingInformation currentInfo,
            ScalingInformation previousInfo,
            int proposedParallelism,
            double targetThroughput,
            Configuration conf) {

        // Get parallelism limits from configuration
        int minParallelism = conf.get(VERTEX_MIN_PARALLELISM);
        int maxParallelism = conf.get(VERTEX_MAX_PARALLELISM);
        
        // Clamp proposed parallelism to valid range
        int clampedProposedParallelism = Math.max(minParallelism, 
                Math.min(maxParallelism, proposedParallelism));

        LOG.info("A4S: Making decision for proposed parallelism={} (clamped from {}), target throughput={}, maxParallelism={}", 
                clampedProposedParallelism, proposedParallelism, targetThroughput, maxParallelism);

        // Check if this is a stateless operator
        if (currentInfo.getAvgCacheHitRate() == 0.0) {
            LOG.info("A4S: Stateless operator detected, using horizontal scaling only");
            return new A4SDecision(clampedProposedParallelism, -1, 0, false, true, 
                    "Stateless operator - horizontal scaling only");
        }

        // Estimate or use cached memory-parallelism curve
        MemoryParallelismCurve mpc = estimateMemoryParallelismCurve(
                currentInfo, targetThroughput, conf);

        // If we have no previous info, this is the first scaling decision
        if (previousInfo == null) {
            return makeFirstDecision(currentInfo, clampedProposedParallelism, mpc, conf, maxParallelism);
        }

        // Check if the previous decision was effective
        boolean previousWasVertical = previousInfo.isVerticalScaling();
        boolean sawImprovement = evaluateImprovementFromPreviousDecision(
                currentInfo, previousInfo, conf);

        if (previousWasVertical) {
            return handlePostVerticalScaling(
                    currentInfo, previousInfo, clampedProposedParallelism, sawImprovement, mpc, conf, maxParallelism);
        } else {
            return handleRegularScaling(
                    currentInfo, previousInfo, clampedProposedParallelism, mpc, conf, maxParallelism);
        }
    }

    /**
     * Make the first scaling decision for an operator.
     */
    private A4SDecision makeFirstDecision(
            ScalingInformation currentInfo,
            int proposedParallelism,
            MemoryParallelismCurve mpc,
            Configuration conf,
            int maxParallelism) {

        LOG.info("A4S: Making first scaling decision (maxParallelism={})", maxParallelism);

        double minCacheHitRate = conf.get(MIN_CACHE_HIT_RATE_THRESHOLD);
        double stateLatencyThreshold = conf.get(STATE_ACCESS_LATENCY_THRESHOLD);

        // Check if vertical scaling might help
        boolean shouldTryVertical = currentInfo.getAvgCacheHitRate() < minCacheHitRate
                || currentInfo.getAvgStateLatency() > stateLatencyThreshold;

        if (shouldTryVertical && proposedParallelism > 1) {
            // Try vertical scaling first - increase memory instead of parallelism
            return selectOptimalFromCurve(mpc, 1, Math.min(proposedParallelism, maxParallelism), conf, 
                    "First decision - trying vertical scaling due to low cache hit rate");
        } else {
            // Use horizontal scaling
            int clampedParallelism = Math.min(proposedParallelism, maxParallelism);
            int memoryLevel = estimateMemoryLevelFromCurve(mpc, clampedParallelism);
            return new A4SDecision(clampedParallelism, memoryLevel, 
                    getMemoryForLevel(memoryLevel), false, true,
                    "First decision - horizontal scaling");
        }
    }

    /**
     * Handle scaling after a previous vertical scaling decision.
     */
    private A4SDecision handlePostVerticalScaling(
            ScalingInformation currentInfo,
            ScalingInformation previousInfo,
            int proposedParallelism,
            boolean sawImprovement,
            MemoryParallelismCurve mpc,
            Configuration conf,
            int maxParallelism) {

        LOG.info("A4S: Previous decision was vertical scaling, improvement={}, maxParallelism={}", 
                sawImprovement, maxParallelism);

        int maxMemoryLevel = conf.get(A4S_MAX_MEMORY_LEVEL);
        double maxCacheHitRate = conf.get(MAX_CACHE_HIT_RATE_THRESHOLD);
        double minImprovedThroughput = conf.get(MIN_IMPROVED_THROUGHPUT);

        // Ensure parallelism doesn't exceed max
        int clampedParallelism = Math.min(proposedParallelism, maxParallelism);

        if (sawImprovement) {
            // Vertical scaling helped, check if we should continue
            boolean throughputImproved = currentInfo.getAvgThroughput() > 
                    previousInfo.getAvgThroughput() * (1.0 + minImprovedThroughput);
            boolean canStillBenefit = currentInfo.getAvgCacheHitRate() < maxCacheHitRate;
            boolean canScaleUp = previousInfo.getMemoryLevel() + 1 < maxMemoryLevel;

            if (throughputImproved && canStillBenefit && canScaleUp) {
                LOG.info("A4S: Continuing vertical scaling - still room for improvement");
                int newMemoryLevel = previousInfo.getMemoryLevel() + 1;
                int parallelism = Math.min(previousInfo.getParallelism(), maxParallelism);
                return new A4SDecision(parallelism, newMemoryLevel,
                        getMemoryForLevel(newMemoryLevel), true, false,
                        "Continuing vertical scaling - throughput improved and cache hit rate below max");
            } else {
                // Reached optimal memory level, now scale horizontally if needed
                LOG.info("A4S: Vertical scaling saturated, switching to horizontal");
                return selectOptimalFromCurve(mpc, clampedParallelism, clampedParallelism, conf,
                        "Vertical scaling saturated - switching to horizontal");
            }
        } else {
            // Vertical scaling didn't help, rollback and try horizontal
            LOG.info("A4S: Vertical scaling didn't help, rolling back");
            int rolledBackLevel = Math.max(0, previousInfo.getMemoryLevel() - 1);
            return new A4SDecision(clampedParallelism, rolledBackLevel,
                    getMemoryForLevel(rolledBackLevel), false, true,
                    "Vertical scaling ineffective - rolling back and scaling horizontally");
        }
    }

    /**
     * Handle regular scaling (not after vertical scaling).
     */
    private A4SDecision handleRegularScaling(
            ScalingInformation currentInfo,
            ScalingInformation previousInfo,
            int proposedParallelism,
            MemoryParallelismCurve mpc,
            Configuration conf,
            int maxParallelism) {

        LOG.info("A4S: Regular scaling decision (proposedParallelism={}, maxParallelism={})", 
                proposedParallelism, maxParallelism);

        double minCacheHitRate = conf.get(MIN_CACHE_HIT_RATE_THRESHOLD);
        double stateLatencyThreshold = conf.get(STATE_ACCESS_LATENCY_THRESHOLD);
        int maxMemoryLevel = conf.get(A4S_MAX_MEMORY_LEVEL);

        // Check if vertical scaling might be beneficial
        boolean lowCacheHitRate = currentInfo.getAvgCacheHitRate() < minCacheHitRate;
        boolean highStateLatency = currentInfo.getAvgStateLatency() > stateLatencyThreshold;
        boolean canScaleVertically = previousInfo.getMemoryLevel() + 1 < maxMemoryLevel
                && !previousInfo.isStopVerticalScaling();

        if ((lowCacheHitRate || highStateLatency) && canScaleVertically) {
            // Try vertical scaling
            LOG.info("A4S: Indicators suggest vertical scaling would help");
            int newMemoryLevel = previousInfo.getMemoryLevel() + 1;
            int parallelism = Math.min(previousInfo.getParallelism(), maxParallelism);
            return new A4SDecision(parallelism, newMemoryLevel,
                    getMemoryForLevel(newMemoryLevel), true, false,
                    "Vertical scaling - cache hit rate or state latency indicates benefit");
        }

        // Use A4S curve to find optimal configuration
        // Ensure we don't exceed maxParallelism
        int minPar = Math.max(1, proposedParallelism / 2);
        int maxPar = Math.min(proposedParallelism * 2, maxParallelism);
        
        LOG.info("A4S: Searching curve with minPar={}, maxPar={}", minPar, maxPar);
        
        return selectOptimalFromCurve(mpc, minPar, maxPar, conf,
                "A4S optimal configuration from memory-parallelism curve");
    }

    /**
     * Select the optimal configuration from the memory-parallelism curve.
     */
    private A4SDecision selectOptimalFromCurve(
            MemoryParallelismCurve mpc,
            int minParallelism,
            int maxParallelism,
            Configuration conf,
            String reason) {

        int maxMemoryLevel = conf.get(A4S_MAX_MEMORY_LEVEL);
        double maxMemoryMB = getMemoryForLevel(maxMemoryLevel);
        double minMemoryMB = getMemoryForLevel(0);

        Optional<MemoryParallelismCurve.CurvePoint> optimal = mpc.findOptimalConfiguration(
                minParallelism, maxParallelism, minMemoryMB, maxMemoryMB);

        if (optimal.isPresent()) {
            MemoryParallelismCurve.CurvePoint point = optimal.get();
            int memoryLevel = memoryMBToLevel(point.getMemoryMB());
            boolean isVertical = memoryLevel > 0;
            
            LOG.info("A4S: Selected optimal configuration from curve: parallelism={}, memoryLevel={}", 
                    point.getParallelism(), memoryLevel);
            
            return new A4SDecision(point.getParallelism(), memoryLevel,
                    point.getMemoryMB(), isVertical, !isVertical || point.getParallelism() > 1,
                    reason);
        }

        // Fallback: use proposed parallelism with minimum memory
        LOG.warn("A4S: No optimal configuration found, using fallback");
        return new A4SDecision(maxParallelism, 0, minMemoryMB, false, true,
                "Fallback - no optimal configuration found on curve");
    }

    /**
     * Evaluate whether the previous scaling decision resulted in improvement.
     */
    private boolean evaluateImprovementFromPreviousDecision(
            ScalingInformation current,
            ScalingInformation previous,
            Configuration conf) {

        double improvedCacheHitThreshold = conf.get(IMPROVED_CACHE_HIT_RATE_THRESHOLD);

        boolean cacheHitImproved = current.getAvgCacheHitRate() - previous.getAvgCacheHitRate() 
                > improvedCacheHitThreshold;
        boolean latencyImproved = current.getAvgStateLatency() < previous.getAvgStateLatency();

        return cacheHitImproved || latencyImproved;
    }

    /**
     * Estimate a memory-parallelism curve based on current metrics.
     *
     * <p>STUB: This should use miss-rate curves derived from QuickMRC.
     * For now, we generate a simplified curve based on observed metrics.
     */
    @VisibleForTesting
    MemoryParallelismCurve estimateMemoryParallelismCurve(
            ScalingInformation info,
            double targetThroughput,
            Configuration conf) {

        LOG.debug("A4S: Estimating memory-parallelism curve for target throughput {}", targetThroughput);

        // STUB: Generate a curve based on assumptions
        // In real implementation, this would use MissRateCurve.deriveMemoryParallelismCurve()
        
        int currentParallelism = info.getParallelism();
        double currentThroughput = info.getAvgThroughput();
        double throughputPerTask = currentThroughput / currentParallelism;

        MemoryParallelismCurve.Builder builder = MemoryParallelismCurve.builder()
                .targetThroughput(targetThroughput);

        // Estimate points based on scaling relationships
        // Higher memory -> higher cache hit rate -> higher throughput per task
        // So we can achieve target throughput with fewer tasks
        
        int minPar = conf.get(VERTEX_MIN_PARALLELISM);
        int maxPar = conf.get(VERTEX_MAX_PARALLELISM);
        int maxMemLevel = conf.get(A4S_MAX_MEMORY_LEVEL);

        for (int memLevel = 0; memLevel <= maxMemLevel; memLevel++) {
            double memoryMB = getMemoryForLevel(memLevel);
            
            // Estimate throughput improvement from more memory
            // Simplified model: each memory level doubles effective throughput per task
            double throughputMultiplier = 1.0 + (memLevel * 0.5);
            double effectiveThroughputPerTask = throughputPerTask * throughputMultiplier;
            
            int requiredParallelism = (int) Math.ceil(targetThroughput / effectiveThroughputPerTask);
            requiredParallelism = Math.max(minPar, Math.min(maxPar, requiredParallelism));
            
            builder.addPoint(requiredParallelism, memoryMB);
        }

        return builder.build();
    }

    /**
     * Estimate the best memory level for a given parallelism using the curve.
     */
    private int estimateMemoryLevelFromCurve(MemoryParallelismCurve mpc, int parallelism) {
        Optional<Double> memoryMB = mpc.getMemoryForParallelism(parallelism);
        return memoryMB.map(this::memoryMBToLevel).orElse(0);
    }

    /**
     * Convert memory in MB to the closest discrete memory level.
     */
    @VisibleForTesting
    int memoryMBToLevel(double memoryMB) {
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
        info.setVerticalScaling(decision.isVerticalScaling());
        info.setHorizontalScaling(decision.isHorizontalScaling());
        
        LOG.info("A4S: Applied decision to scaling info: {}", decision);
    }
}
