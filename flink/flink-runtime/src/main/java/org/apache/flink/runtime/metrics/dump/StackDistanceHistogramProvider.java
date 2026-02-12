/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.metrics.dump;

import org.apache.flink.metrics.Metric;

/**
 * A metric provider for stack distance histograms. Implementations supply fixed-size bucket
 * boundaries and on-demand bucket counts fetched from a backing store (e.g. RocksDB).
 *
 * <p>Unlike regular Flink metrics that are periodically pushed, stack distance histograms are
 * fetched on demand when the JobManager queries TaskManagers.
 */
public interface StackDistanceHistogramProvider extends Metric {

    /**
     * Fetches the current bucket counts on demand from the backing store.
     *
     * <p>The returned array has {@code getBucketBoundaries().length + 1} entries: one count per
     * bucket interval plus an overflow bucket for values exceeding the last boundary.
     *
     * @return the current bucket counts
     */
    long[] fetchBucketCounts();

    /**
     * Returns the fixed bucket boundaries for this histogram.
     *
     * @return the bucket boundaries array
     */
    long[] getBucketBoundaries();
}
