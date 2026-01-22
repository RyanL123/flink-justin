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
 * Memory-Parallelism Curve (MPC) as described in the A4S paper.
 *
 * <p>The curve shows the minimum operator memory size needed for a given level of parallelism
 * to achieve a target throughput. This allows trading memory for parallelism and vice versa.
 *
 * <p>Key insight: For stateful operators, memory and parallelism are correlated when
 * achieving a certain throughput. More memory can reduce IO operations (cache hits),
 * potentially allowing lower parallelism to achieve the same throughput.
 */
public class MemoryParallelismCurve {

    private static final Logger LOG = LoggerFactory.getLogger(MemoryParallelismCurve.class);

    /** Target throughput (records/sec) this curve is generated for. */
    @Getter
    private final double targetThroughput;

    /** List of (parallelism, memory) points on the curve, sorted by parallelism ascending. */
    @Getter
    private final List<CurvePoint> points;

    /** Minimum parallelism on this curve. */
    @Getter
    private final int minParallelism;

    /** Maximum parallelism on this curve. */
    @Getter
    private final int maxParallelism;

    /**
     * Represents a single point on the memory-parallelism curve.
     */
    public static class CurvePoint implements Comparable<CurvePoint> {
        @Getter
        private final int parallelism;
        
        /** Memory in MB. */
        @Getter
        private final double memoryMB;

        public CurvePoint(int parallelism, double memoryMB) {
            this.parallelism = parallelism;
            this.memoryMB = memoryMB;
        }

        @Override
        public int compareTo(CurvePoint other) {
            return Integer.compare(this.parallelism, other.parallelism);
        }

        @Override
        public String toString() {
            return String.format("(p=%d, mem=%.2fMB)", parallelism, memoryMB);
        }
    }

    public MemoryParallelismCurve(double targetThroughput, List<CurvePoint> points) {
        this.targetThroughput = targetThroughput;
        this.points = new ArrayList<>(points);
        Collections.sort(this.points);
        
        if (this.points.isEmpty()) {
            this.minParallelism = 1;
            this.maxParallelism = 1;
        } else {
            this.minParallelism = this.points.get(0).getParallelism();
            this.maxParallelism = this.points.get(this.points.size() - 1).getParallelism();
        }
    }

    /**
     * Get the minimum memory required for a given parallelism level.
     *
     * @param parallelism the parallelism level
     * @return the minimum memory in MB, or empty if parallelism is out of range
     */
    public Optional<Double> getMemoryForParallelism(int parallelism) {
        if (parallelism < minParallelism || parallelism > maxParallelism) {
            return Optional.empty();
        }

        // Find the point with exact parallelism or interpolate
        for (int i = 0; i < points.size(); i++) {
            CurvePoint point = points.get(i);
            if (point.getParallelism() == parallelism) {
                return Optional.of(point.getMemoryMB());
            }
            if (point.getParallelism() > parallelism && i > 0) {
                // Interpolate between points[i-1] and points[i]
                CurvePoint prev = points.get(i - 1);
                double ratio = (double) (parallelism - prev.getParallelism()) 
                        / (point.getParallelism() - prev.getParallelism());
                double memory = prev.getMemoryMB() + ratio * (point.getMemoryMB() - prev.getMemoryMB());
                return Optional.of(memory);
            }
        }
        
        return Optional.empty();
    }

    /**
     * Get the parallelism needed for a given memory constraint.
     *
     * @param availableMemoryMB the available memory in MB
     * @return the minimum parallelism needed, or empty if memory is insufficient
     */
    public Optional<Integer> getParallelismForMemory(double availableMemoryMB) {
        // Find the lowest parallelism where required memory <= available memory
        for (CurvePoint point : points) {
            if (point.getMemoryMB() <= availableMemoryMB) {
                return Optional.of(point.getParallelism());
            }
        }
        
        // If no point fits, return the highest parallelism (lowest memory requirement)
        if (!points.isEmpty()) {
            CurvePoint lastPoint = points.get(points.size() - 1);
            if (lastPoint.getMemoryMB() <= availableMemoryMB) {
                return Optional.of(lastPoint.getParallelism());
            }
        }
        
        return Optional.empty();
    }

    /**
     * Find all valid (parallelism, memory) configurations within given constraints.
     *
     * @param minPar minimum parallelism constraint
     * @param maxPar maximum parallelism constraint
     * @param minMemoryMB minimum memory constraint (MB)
     * @param maxMemoryMB maximum memory constraint (MB)
     * @return list of valid curve points within constraints
     */
    public List<CurvePoint> getValidConfigurations(
            int minPar, int maxPar, double minMemoryMB, double maxMemoryMB) {
        List<CurvePoint> valid = new ArrayList<>();
        
        for (CurvePoint point : points) {
            if (point.getParallelism() >= minPar 
                    && point.getParallelism() <= maxPar
                    && point.getMemoryMB() >= minMemoryMB 
                    && point.getMemoryMB() <= maxMemoryMB) {
                valid.add(point);
            }
        }
        
        return valid;
    }

    /**
     * Calculate the total resource cost for a given configuration.
     * Cost = parallelism * memory (simplified resource model).
     *
     * @param parallelism the parallelism level
     * @param memoryMB the memory in MB
     * @return the resource cost
     */
    public static double calculateResourceCost(int parallelism, double memoryMB) {
        return parallelism * memoryMB;
    }

    /**
     * Find the optimal configuration that minimizes resource cost.
     *
     * @param minPar minimum parallelism
     * @param maxPar maximum parallelism
     * @param minMemoryMB minimum memory (MB)
     * @param maxMemoryMB maximum memory (MB)
     * @return the optimal configuration, or empty if none exists
     */
    public Optional<CurvePoint> findOptimalConfiguration(
            int minPar, int maxPar, double minMemoryMB, double maxMemoryMB) {
        List<CurvePoint> valid = getValidConfigurations(minPar, maxPar, minMemoryMB, maxMemoryMB);
        
        if (valid.isEmpty()) {
            return Optional.empty();
        }
        
        CurvePoint optimal = valid.get(0);
        double minCost = calculateResourceCost(optimal.getParallelism(), optimal.getMemoryMB());
        
        for (CurvePoint point : valid) {
            double cost = calculateResourceCost(point.getParallelism(), point.getMemoryMB());
            if (cost < minCost) {
                minCost = cost;
                optimal = point;
            }
        }
        
        LOG.debug("Found optimal configuration: {} with cost {}", optimal, minCost);
        return Optional.of(optimal);
    }

    @Override
    public String toString() {
        return String.format("MPC[target=%.2f rec/s, points=%s]", targetThroughput, points);
    }

    /**
     * Builder for creating MemoryParallelismCurve instances.
     */
    public static class Builder {
        private double targetThroughput;
        private final List<CurvePoint> points = new ArrayList<>();

        public Builder targetThroughput(double throughput) {
            this.targetThroughput = throughput;
            return this;
        }

        public Builder addPoint(int parallelism, double memoryMB) {
            points.add(new CurvePoint(parallelism, memoryMB));
            return this;
        }

        public MemoryParallelismCurve build() {
            return new MemoryParallelismCurve(targetThroughput, points);
        }
    }

    @VisibleForTesting
    public static Builder builder() {
        return new Builder();
    }
}
