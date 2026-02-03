/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.rocksdb.RocksDBMRCMetricsProvider;
import org.apache.flink.runtime.state.rocksdb.RocksDBMRCMetricsSnapshot;

import org.rocksdb.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * TaskManager-side implementation of {@link RocksDBMRCMetricsProvider}. Reads bucket statistics
 * from the RocksDB block cache via reflection (when the cache is frocksdb LRUCache with ghost
 * cache). Used so the JobManager can request MRC metrics from the TaskManager over RPC.
 */
@Internal
public final class RocksDBMRCMetricsProviderImpl implements RocksDBMRCMetricsProvider {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMRCMetricsProviderImpl.class);

    /** Non-serialized; only used on the TaskManager where the provider is registered. */
    private transient Cache cache;

    public RocksDBMRCMetricsProviderImpl(Cache cache) {
        this.cache = cache;
    }

    @Override
    public RocksDBMRCMetricsSnapshot getBucketStatisticsSnapshot() {
        if (cache == null) {
            return new RocksDBMRCMetricsSnapshot(new long[0], new long[0], new long[0]);
        }
        try {
            Method getBucketStatistics = cache.getClass().getMethod("getBucketStatistics");
            Object stats = getBucketStatistics.invoke(cache);
            if (stats == null) {
                return new RocksDBMRCMetricsSnapshot(new long[0], new long[0], new long[0]);
            }
            Class<?> statsClass = stats.getClass();
            Method sizeMethod = statsClass.getMethod("size");
            int n = (Integer) sizeMethod.invoke(stats);
            if (n <= 0) {
                return new RocksDBMRCMetricsSnapshot(new long[0], new long[0], new long[0]);
            }
            long[] cacheSizes = copyLongArray(stats, statsClass, "getCacheSizes", n);
            long[] hits = copyLongArray(stats, statsClass, "getHits", n);
            long[] misses = copyLongArray(stats, statsClass, "getMisses", n);
            return new RocksDBMRCMetricsSnapshot(cacheSizes, hits, misses);
        } catch (NoSuchMethodException e) {
            LOG.trace("Cache does not support getBucketStatistics (stock RocksDB)");
            return new RocksDBMRCMetricsSnapshot(new long[0], new long[0], new long[0]);
        } catch (Exception e) {
            LOG.warn("Failed to get RocksDB MRC bucket statistics", e);
            return new RocksDBMRCMetricsSnapshot(new long[0], new long[0], new long[0]);
        }
    }

    private static long[] copyLongArray(Object stats, Class<?> statsClass, String getterName, int len)
            throws Exception {
        Method m = statsClass.getMethod(getterName);
        long[] arr = (long[]) m.invoke(stats);
        if (arr == null || arr.length < len) {
            return new long[0];
        }
        long[] out = new long[len];
        System.arraycopy(arr, 0, out, 0, len);
        return out;
    }
}
