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

package org.apache.flink.runtime.state.rocksdb;

import java.io.Serializable;

/**
 * Provider of RocksDB MRC (miss-rate curve) bucket statistics for a slot. Implemented by the
 * RocksDB state backend when ghost cache is enabled; the TaskManager holds registered providers
 * and returns snapshots to the JobManager over RPC.
 */
public interface RocksDBMRCMetricsProvider extends Serializable {

    /**
     * Returns a snapshot of bucket statistics (cache sizes, hits, misses per bucket) for this
     * slot's RocksDB block cache. The JobManager can aggregate these across slots.
     */
    RocksDBMRCMetricsSnapshot getBucketStatisticsSnapshot();
}
