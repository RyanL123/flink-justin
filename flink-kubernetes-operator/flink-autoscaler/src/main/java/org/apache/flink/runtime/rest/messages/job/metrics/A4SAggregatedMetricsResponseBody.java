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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/** A4S response body containing aggregated scalar metrics and scaled MRC points. */
public class A4SAggregatedMetricsResponseBody implements ResponseBody {

    public static final String FIELD_NAME_METRICS = "metrics";
    public static final String FIELD_NAME_SCALED_MRC = "scaledMrc";

    @JsonProperty(FIELD_NAME_METRICS)
    private final Collection<AggregatedMetric> metrics;

    @JsonProperty(FIELD_NAME_SCALED_MRC)
    private final List<MRCPoint> scaledMrc;

    @JsonCreator
    public A4SAggregatedMetricsResponseBody(
            @JsonProperty(FIELD_NAME_METRICS) Collection<AggregatedMetric> metrics,
            @JsonProperty(FIELD_NAME_SCALED_MRC) List<MRCPoint> scaledMrc) {
        this.metrics = metrics == null ? Collections.emptyList() : metrics;
        this.scaledMrc = scaledMrc == null ? Collections.emptyList() : scaledMrc;
    }

    public Collection<AggregatedMetric> getMetrics() {
        return metrics;
    }

    public List<MRCPoint> getScaledMrc() {
        return scaledMrc;
    }

    /** A single point on the scaled MRC curve. */
    public static class MRCPoint {

        public static final String FIELD_NAME_CACHE_SIZE_BYTES = "cacheSizeBytes";
        public static final String FIELD_NAME_MISS_RATE = "missRate";

        @JsonProperty(FIELD_NAME_CACHE_SIZE_BYTES)
        private final long cacheSizeBytes;

        @JsonProperty(FIELD_NAME_MISS_RATE)
        private final double missRate;

        @JsonCreator
        public MRCPoint(
                @JsonProperty(FIELD_NAME_CACHE_SIZE_BYTES) long cacheSizeBytes,
                @JsonProperty(FIELD_NAME_MISS_RATE) double missRate) {
            this.cacheSizeBytes = cacheSizeBytes;
            this.missRate = missRate;
        }

        public long getCacheSizeBytes() {
            return cacheSizeBytes;
        }

        public double getMissRate() {
            return missRate;
        }
    }
}
