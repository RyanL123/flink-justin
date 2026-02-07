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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.state.rocksdb.RocksDBMRCMetricsProvider;
import org.apache.flink.runtime.state.rocksdb.RocksDBMRCMetricsProviderRegistration;

import java.util.Map;

/**
 * Registration implementation used by the TaskExecutor so that the RocksDB state backend (in a
 * task) can register its MRC metrics provider for the slot. The TaskExecutor stores providers by
 * AllocationID and serves them to the JobManager via RPC.
 */
public final class TaskExecutorRocksDBMRCRegistration
        implements RocksDBMRCMetricsProviderRegistration {

    private final Map<AllocationID, RocksDBMRCMetricsProvider> providers;
    private final AllocationID allocationId;

    public TaskExecutorRocksDBMRCRegistration(
            Map<AllocationID, RocksDBMRCMetricsProvider> providers, AllocationID allocationId) {
        this.providers = providers;
        this.allocationId = allocationId;
    }

    @Override
    public void registerRocksDBMRCMetricsProvider(RocksDBMRCMetricsProvider provider) {
        providers.put(allocationId, provider);
    }
}
