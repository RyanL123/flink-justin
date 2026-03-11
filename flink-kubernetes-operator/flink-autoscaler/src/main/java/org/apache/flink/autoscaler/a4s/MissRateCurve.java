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

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * Miss-Rate Curve (MRC) as described in the A4S paper.
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
        @Getter
        private final double cacheSizeMb;
        
        /** Miss rate (0.0 - 1.0). */
        @Getter
        private final double missRate;

        @JsonCreator
        public MRCPoint(
                @JsonProperty("cacheSizeMb") double cacheSizeMb,
                @JsonProperty("missRate") double missRate) {
            this.cacheSizeMb = cacheSizeMb;
            this.missRate = Math.max(0.0, Math.min(1.0, missRate));
        }

        @Override
        public int compareTo(MRCPoint other) {
            return Double.compare(this.cacheSizeMb, other.cacheSizeMb);
        }

        @Override
        public String toString() {
            return String.format("(size=%.2fmb, mr=%.4f)", cacheSizeMb, missRate);
        }
    }

    @JsonCreator
    public MissRateCurve(@JsonProperty("points") List<MRCPoint> points) {
        this.points = new ArrayList<>(points);
        Collections.sort(this.points);
    }

    @Override
    public String toString() {
        return String.format("MRC[points=%s]", points);
    }

    /**
     * Find the least cache size (memory in MB) that achieves a miss rate at or below the maximum.
     *
     * <p>The curve points are sorted by cache size ascending, with miss rate typically
     * decreasing as cache size increases. This method finds the smallest cache size
     * where the miss rate is <= maxMissRate.
     *
     * @param maxMissRate the maximum acceptable miss rate (0.0 - 1.0)
     * @return the least memory in MB that achieves the required miss rate, or empty if
     *         the curve has no points or no point achieves the required miss rate
     */
    public Optional<Double> leastMemoryMbForMissRate(double maxMissRate) {
        if (points.isEmpty()) {
            throw new IllegalStateException("MissRateCurve has no points");
        }

        Optional<MRCPoint> bestPoint = points.stream().
            filter(point -> point.getMissRate() <= maxMissRate).
            min(Comparator.comparingDouble(MRCPoint::getCacheSizeMb));

        if (bestPoint.isEmpty()) {
            LOG.warn("No point found for miss rate {}, returning empty", maxMissRate);
            return Optional.empty();
        }

        return bestPoint.map(MRCPoint::getCacheSizeMb);
    }

    /**
     * Builder for creating MissRateCurve instances.
     */
    public static class Builder {
        private final List<MRCPoint> points = new ArrayList<>();

        public Builder addPoint(double cacheSizeMb, double missRate) {
            points.add(new MRCPoint(cacheSizeMb, missRate));
            return this;
        }

        public MissRateCurve build() {
            return new MissRateCurve(points);
        }
    }
}
