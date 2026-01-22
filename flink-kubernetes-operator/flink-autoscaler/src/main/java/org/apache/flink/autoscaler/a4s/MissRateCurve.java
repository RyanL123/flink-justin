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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Miss-Rate Curve (MRC) as described in the A4S paper.
 *
 * <p>The miss-rate curve shows the cache miss rate as a function of cache size.
 * It is used to derive the memory-parallelism curve for stateful operators.
 *
 * <p>Key insight from A4S: The miss-rate curve can be generated online using
 * techniques like QuickMRC with ghost caches and stack distance estimation.
 *
 * <p>NOTE: This is a stub implementation. The actual curve generation using
 * stack distances (as described in the A4S paper sections 3.4.1-3.4.2) is not
 * implemented yet.
 */
public class MissRateCurve {

    private static final Logger LOG = LoggerFactory.getLogger(MissRateCurve.class);

    /** List of (cacheSize, missRate) points on the curve, sorted by cache size ascending. */
    @Getter
    private final List<MRCPoint> points;

    /**
     * Represents a single point on the miss-rate curve.
     */
    public static class MRCPoint implements Comparable<MRCPoint> {
        /** Cache size in number of items or MB. */
        @Getter
        private final double cacheSize;
        
        /** Miss rate (0.0 - 1.0). */
        @Getter
        private final double missRate;

        public MRCPoint(double cacheSize, double missRate) {
            this.cacheSize = cacheSize;
            this.missRate = Math.max(0.0, Math.min(1.0, missRate));
        }

        @Override
        public int compareTo(MRCPoint other) {
            return Double.compare(this.cacheSize, other.cacheSize);
        }

        @Override
        public String toString() {
            return String.format("(size=%.2f, mr=%.4f)", cacheSize, missRate);
        }
    }

    public MissRateCurve(List<MRCPoint> points) {
        this.points = new ArrayList<>(points);
        Collections.sort(this.points);
    }

    /**
     * Get the miss rate for a given cache size.
     *
     * @param cacheSize the cache size
     * @return the miss rate, interpolated if necessary
     */
    public double getMissRate(double cacheSize) {
        if (points.isEmpty()) {
            return 1.0; // No cache = 100% miss rate
        }

        // Binary search for the closest points
        int idx = Collections.binarySearch(points, new MRCPoint(cacheSize, 0));
        
        if (idx >= 0) {
            return points.get(idx).getMissRate();
        }
        
        int insertionPoint = -(idx + 1);
        
        if (insertionPoint == 0) {
            return points.get(0).getMissRate();
        }
        if (insertionPoint >= points.size()) {
            return points.get(points.size() - 1).getMissRate();
        }
        
        // Linear interpolation
        MRCPoint lower = points.get(insertionPoint - 1);
        MRCPoint upper = points.get(insertionPoint);
        
        double ratio = (cacheSize - lower.getCacheSize()) 
                / (upper.getCacheSize() - lower.getCacheSize());
        return lower.getMissRate() + ratio * (upper.getMissRate() - lower.getMissRate());
    }

    /**
     * Get the hit rate for a given cache size.
     *
     * @param cacheSize the cache size
     * @return the hit rate (1 - miss rate)
     */
    public double getHitRate(double cacheSize) {
        return 1.0 - getMissRate(cacheSize);
    }

    /**
     * Find the minimum cache size needed to achieve a target miss rate.
     *
     * @param targetMissRate the target miss rate (0.0 - 1.0)
     * @return the minimum cache size, or empty if not achievable
     */
    public Optional<Double> getCacheSizeForMissRate(double targetMissRate) {
        if (points.isEmpty()) {
            return Optional.empty();
        }

        // Find the smallest cache size where miss rate <= target
        for (MRCPoint point : points) {
            if (point.getMissRate() <= targetMissRate) {
                return Optional.of(point.getCacheSize());
            }
        }
        
        // Target miss rate too low to achieve
        return Optional.empty();
    }

    /**
     * Derive a memory-parallelism curve from this miss-rate curve.
     *
     * <p>This implements the relationship described in A4S paper Section 3.2:
     * For a given target throughput, we can trade memory (cache size) for parallelism.
     *
     * @param targetThroughput target throughput in records/sec
     * @param baseProcessingRate processing rate per task with 100% cache hit
     * @param ioLatencyMs IO latency in milliseconds for cache misses
     * @param minParallelism minimum parallelism to consider
     * @param maxParallelism maximum parallelism to consider
     * @return the derived memory-parallelism curve
     */
    public MemoryParallelismCurve deriveMemoryParallelismCurve(
            double targetThroughput,
            double baseProcessingRate,
            double ioLatencyMs,
            int minParallelism,
            int maxParallelism) {
        
        MemoryParallelismCurve.Builder builder = MemoryParallelismCurve.builder()
                .targetThroughput(targetThroughput);

        // For each parallelism level, compute required memory
        for (int p = minParallelism; p <= maxParallelism; p++) {
            double requiredThroughputPerTask = targetThroughput / p;
            
            // Compute required hit rate to achieve throughput
            // effectiveRate = baseRate * hitRate + (baseRate * missRate) / (1 + ioLatency)
            // Simplified: we need hitRate such that effectiveRate >= requiredThroughputPerTask
            double requiredHitRate = computeRequiredHitRate(
                    requiredThroughputPerTask, baseProcessingRate, ioLatencyMs);
            
            if (requiredHitRate > 1.0) {
                // Cannot achieve this throughput even with 100% hit rate
                continue;
            }
            
            double requiredMissRate = 1.0 - requiredHitRate;
            Optional<Double> cacheSize = getCacheSizeForMissRate(requiredMissRate);
            
            if (cacheSize.isPresent()) {
                builder.addPoint(p, cacheSize.get());
            }
        }

        return builder.build();
    }

    /**
     * Compute the required hit rate to achieve a target throughput per task.
     *
     * <p>Based on the queuing model in A4S paper.
     */
    private double computeRequiredHitRate(
            double requiredThroughput, double baseRate, double ioLatencyMs) {
        if (ioLatencyMs <= 0) {
            return 0.0; // No IO penalty, any hit rate works
        }
        
        // Simplified model: effectiveRate = baseRate / (1 + missRate * ioLatencyFactor)
        // Solving for hitRate: hitRate = 1 - ((baseRate / requiredThroughput) - 1) / ioLatencyFactor
        double ioLatencyFactor = ioLatencyMs / 1000.0; // Convert to seconds
        double ratio = baseRate / requiredThroughput;
        
        if (ratio <= 1.0) {
            return 0.0; // Base rate already sufficient
        }
        
        double requiredMissRateReduction = (ratio - 1.0) / ioLatencyFactor;
        return Math.max(0.0, 1.0 - requiredMissRateReduction);
    }

    @Override
    public String toString() {
        return String.format("MRC[points=%s]", points);
    }

    /**
     * Builder for creating MissRateCurve instances.
     */
    public static class Builder {
        private final List<MRCPoint> points = new ArrayList<>();

        public Builder addPoint(double cacheSize, double missRate) {
            points.add(new MRCPoint(cacheSize, missRate));
            return this;
        }

        public MissRateCurve build() {
            return new MissRateCurve(points);
        }
    }

    @VisibleForTesting
    public static Builder builder() {
        return new Builder();
    }

    /**
     * STUB: Generate a miss-rate curve from observed metrics.
     *
     * <p>In the actual A4S implementation, this would use QuickMRC algorithm
     * with ghost caches and stack distance estimation (see paper Section 3.4).
     *
     * @param observedHitRate the currently observed cache hit rate
     * @param currentCacheSize the current cache size in MB
     * @return an estimated miss-rate curve
     */
    public static MissRateCurve estimateFromMetrics(double observedHitRate, double currentCacheSize) {
        LOG.debug("Estimating MRC from hitRate={}, cacheSize={}", observedHitRate, currentCacheSize);
        
        // STUB: Create a simple exponential decay curve based on observed point
        // Real implementation would use stack distance analysis
        double observedMissRate = 1.0 - observedHitRate;
        
        Builder builder = new Builder();
        
        // Generate points assuming exponential decay: MR(s) = MR(s0) * (s0/s)^alpha
        // where alpha controls the curve shape
        double alpha = 0.5; // Typical value, should be learned from data
        
        for (double sizeFactor = 0.25; sizeFactor <= 4.0; sizeFactor += 0.25) {
            double size = currentCacheSize * sizeFactor;
            double missRate;
            
            if (sizeFactor <= 1.0) {
                // Smaller cache = higher miss rate
                missRate = Math.min(1.0, observedMissRate * Math.pow(1.0 / sizeFactor, alpha));
            } else {
                // Larger cache = lower miss rate
                missRate = observedMissRate * Math.pow(1.0 / sizeFactor, alpha);
            }
            
            builder.addPoint(size, missRate);
        }
        
        return builder.build();
    }
}
