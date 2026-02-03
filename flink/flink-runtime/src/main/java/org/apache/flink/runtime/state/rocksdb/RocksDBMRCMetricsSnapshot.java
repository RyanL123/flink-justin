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
 * Serializable snapshot of RocksDB miss-rate curve (MRC) bucket statistics. Can be sent from
 * TaskManager to JobManager over RPC. Used for aggregating MRC data across slots/tasks.
 */
public class RocksDBMRCMetricsSnapshot implements Serializable {

    private static final long serialVersionUID = 1L;

    private final long[] cacheSizes;
    private final long[] hits;
    private final long[] misses;

    public RocksDBMRCMetricsSnapshot(long[] cacheSizes, long[] hits, long[] misses) {
        this.cacheSizes = cacheSizes != null ? cacheSizes.clone() : new long[0];
        this.hits = hits != null ? hits.clone() : new long[0];
        this.misses = misses != null ? misses.clone() : new long[0];
    }

    public long[] getCacheSizes() {
        return cacheSizes.clone();
    }

    public long[] getHits() {
        return hits.clone();
    }

    public long[] getMisses() {
        return misses.clone();
    }

    public int size() {
        return cacheSizes.length;
    }
}
