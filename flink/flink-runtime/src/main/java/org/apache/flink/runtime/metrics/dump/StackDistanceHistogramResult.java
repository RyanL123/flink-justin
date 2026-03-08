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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * Serializable result of querying a stack distance histogram from a TaskManager. Returned by {@link
 * MetricQueryService#queryStackDistanceHistograms} via RPC.
 */
public class StackDistanceHistogramResult implements Serializable {

    private static final long serialVersionUID = 1L;

    private final QueryScopeInfo scopeInfo;
    private final String name;
    private final long[] bucketCounts;

    public StackDistanceHistogramResult(
            QueryScopeInfo scopeInfo,
            String name,
            long[] bucketCounts) {
        this.scopeInfo = Preconditions.checkNotNull(scopeInfo);
        this.name = Preconditions.checkNotNull(name);
        this.bucketCounts = Preconditions.checkNotNull(bucketCounts);
    }

    public QueryScopeInfo getScopeInfo() {
        return scopeInfo;
    }

    public String getName() {
        return name;
    }

    public long[] getBucketCounts() {
        return bucketCounts;
    }
}
