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

/**
 * Registration interface exposed to the task so that the RocksDB state backend (on the TaskManager)
 * can register its MRC metrics provider. The TaskExecutor uses this to serve MRC data to the
 * JobManager over RPC.
 */
public interface RocksDBMRCMetricsProviderRegistration {

    /**
     * Register a provider for the current slot. Called by the RocksDB state backend when it
     * creates a keyed state backend with MRC/ghost cache enabled. Only one provider per slot
     * is expected; subsequent registrations may replace the previous one.
     */
    void registerRocksDBMRCMetricsProvider(RocksDBMRCMetricsProvider provider);
}
