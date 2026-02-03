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

import org.rocksdb.Cache;
import org.rocksdb.LRUCache;
import org.rocksdb.WriteBufferManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * A {@link RocksDBMemoryControllerUtils.RocksDBMemoryFactory} that creates an LRUCache and
 * optionally enables the ghost cache for miss-rate curve (MRC) generation when the RocksDB build
 * supports it (e.g. capstone frocksdb with enableGhostCache).
 */
@Internal
public class RocksDBMRCGhostCacheFactory implements RocksDBMemoryControllerUtils.RocksDBMemoryFactory {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBMRCGhostCacheFactory.class);

    private static final long serialVersionUID = 1L;

    private final boolean ghostCacheEnabled;
    private final double ghostCacheCapacityRatio;
    private final long[] distanceBuckets;

    public RocksDBMRCGhostCacheFactory(
            boolean ghostCacheEnabled,
            double ghostCacheCapacityRatio,
            long[] distanceBuckets) {
        this.ghostCacheEnabled = ghostCacheEnabled;
        this.ghostCacheCapacityRatio = ghostCacheCapacityRatio;
        this.distanceBuckets = distanceBuckets != null ? distanceBuckets.clone() : new long[0];
    }

    boolean isGhostCacheEnabled() {
        return ghostCacheEnabled;
    }

    double getGhostCacheCapacityRatio() {
        return ghostCacheCapacityRatio;
    }

    long[] getDistanceBuckets() {
        return distanceBuckets.clone();
    }

    @Override
    public Cache createCache(long cacheCapacity, double highPriorityPoolRatio) {
        Cache cache =
                RocksDBMemoryControllerUtils.createCache(cacheCapacity, highPriorityPoolRatio);
        if (ghostCacheEnabled && cache instanceof LRUCache && distanceBuckets.length > 0) {
            enableGhostCacheIfSupported((LRUCache) cache, cacheCapacity);
        }
        return cache;
    }

    @Override
    public WriteBufferManager createWriteBufferManager(
            long writeBufferManagerCapacity, Cache cache) {
        return RocksDBMemoryControllerUtils.createWriteBufferManager(
                writeBufferManagerCapacity, cache);
    }

    private void enableGhostCacheIfSupported(LRUCache lruCache, long cacheCapacity) {
        try {
            Method m = lruCache.getClass().getMethod("enableGhostCache", long.class, long[].class);
            long ghostCapacity = (long) (cacheCapacity * ghostCacheCapacityRatio);
            m.invoke(lruCache, ghostCapacity, distanceBuckets);
            LOG.info(
                    "Enabled RocksDB ghost cache for MRC: ghostCapacity={}, buckets={}",
                    ghostCapacity,
                    distanceBuckets.length);
        } catch (NoSuchMethodException e) {
            LOG.debug(
                    "RocksDB LRUCache does not support enableGhostCache (stock build); MRC metrics will be unavailable.");
        } catch (Exception e) {
            LOG.warn("Failed to enable RocksDB ghost cache for MRC", e);
        }
    }
}
