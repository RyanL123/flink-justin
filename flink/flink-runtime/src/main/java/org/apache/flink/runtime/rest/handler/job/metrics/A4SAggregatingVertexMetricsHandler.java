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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedVertexMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

/**
 * Request handler that returns A4S-specific metrics including parallelism and
 * ResourceProfile
 * information, aggregated across subtasks of a job vertex.
 *
 * <p>
 * This handler extends the standard aggregating metrics handler to include:
 * <ul>
 * <li>Current parallelism per vertex</li>
 * <li>Max parallelism per vertex</li>
 * <li>ResourceProfile values (taskHeapMemory, taskOffHeapMemory, managedMemory,
 * networkMemory, totalMemory, operatorsMemory)</li>
 * <li>Memory consumption metrics per subtask</li>
 * </ul>
 *
 * <p>
 * Usage:
 * {@code /jobs/:jobid/vertices/:vertexid/a4s-metrics?get=parallelism,resourceProfile.totalMemory}
 */
public class A4SAggregatingVertexMetricsHandler
        extends AbstractAggregatingMetricsHandler<AggregatedSubtaskMetricsParameters> {

    private final Executor executor;
    private final MetricFetcher fetcher;
    private final ExecutionGraphCache executionGraphCache;

    public A4SAggregatingVertexMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher,
            ExecutionGraphCache executionGraphCache) {
        super(leaderRetriever, timeout, responseHeaders,
                A4SAggregatedVertexMetricsHeaders.getInstance(), executor, fetcher);
        this.executor = executor;
        this.fetcher = fetcher;
        this.executionGraphCache = executionGraphCache;
    }

    @Nonnull
    @Override
    Collection<? extends MetricStore.ComponentMetricStore> getStores(
            MetricStore store, HandlerRequest<EmptyRequestBody> request) {
        JobID jobID = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID taskID = request.getPathParameter(JobVertexIdPathParameter.class);

        MetricStore.TaskMetricStore taskMetricStore = store.getTaskMetricStore(jobID.toString(), taskID.toString());
        if (taskMetricStore != null) {
            return taskMetricStore.getAllSubtaskMetricStores().values();
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    protected CompletableFuture<AggregatedMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        return executionGraphCache.getExecutionGraphInfo(
                request.getPathParameter(JobIDPathParameter.class), gateway)
                .thenCompose(executionGraphInfo -> CompletableFuture.supplyAsync(() -> {
                    try {
                        this.fetcher.update();
                        MetricStore store = this.fetcher.getMetricStore();

                        JobVertexID vertexID = request.getPathParameter(JobVertexIdPathParameter.class);

                        // Get vertex information from ExecutionGraph
                        AccessExecutionGraph executionGraph = executionGraphInfo.getArchivedExecutionGraph();
                        AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(vertexID);

                        if (jobVertex == null) {
                            throw new CompletionException(new RestHandlerException(
                                    String.format("JobVertex %s not found", vertexID),
                                    HttpResponseStatus.NOT_FOUND));
                        }

                        // A4S metrics will be added on-the-fly during aggregation

                        // Now use parent's handleRequest logic
                        // We need to call the parent's implementation
                        // but since it's not accessible, we'll implement
                        // the logic here
                        List<String> requestedMetrics = request.getQueryParameter(MetricsFilterParameter.class);
                        List<MetricsAggregationParameter.AggregationMode> requestedAggregations = request
                                .getQueryParameter(MetricsAggregationParameter.class);

                        Collection<? extends MetricStore.ComponentMetricStore> stores = getStores(store, request);

                        if (requestedMetrics.isEmpty()) {
                            Set<String> uniqueMetrics = CollectionUtil.newHashSetWithExpectedSize(32);
                            for (MetricStore.ComponentMetricStore storeItem : stores) {
                                uniqueMetrics.addAll(storeItem.metrics.keySet());
                            }
                            // Add A4S-specific metrics to the list
                            uniqueMetrics.addAll(getA4SMetricNames());
                            return new AggregatedMetricsResponseBody(uniqueMetrics.stream()
                                    .map(AggregatedMetric::new)
                                    .collect(Collectors.toList()));
                        }

                        // Create accumulator factories
                        DoubleAccumulator.DoubleMinimumFactory minimumFactory = null;
                        DoubleAccumulator.DoubleMaximumFactory maximumFactory = null;
                        DoubleAccumulator.DoubleAverageFactory averageFactory = null;
                        DoubleAccumulator.DoubleSumFactory sumFactory = null;

                        if (requestedAggregations.isEmpty()) {
                            minimumFactory = DoubleAccumulator.DoubleMinimumFactory.get();
                            maximumFactory = DoubleAccumulator.DoubleMaximumFactory.get();
                            averageFactory = DoubleAccumulator.DoubleAverageFactory.get();
                            sumFactory = DoubleAccumulator.DoubleSumFactory.get();
                        } else {
                            for (MetricsAggregationParameter.AggregationMode aggregation : requestedAggregations) {
                                switch (aggregation) {
                                    case MIN:
                                        minimumFactory = DoubleAccumulator.DoubleMinimumFactory.get();
                                        break;
                                    case MAX:
                                        maximumFactory = DoubleAccumulator.DoubleMaximumFactory.get();
                                        break;
                                    case AVG:
                                        averageFactory = DoubleAccumulator.DoubleAverageFactory.get();
                                        break;
                                    case SUM:
                                        sumFactory = DoubleAccumulator.DoubleSumFactory.get();
                                        break;
                                    default:
                                        log.warn("Unsupported aggregation specified: {}", aggregation);
                                }
                            }
                        }

                        MetricAccumulatorFactory metricAccumulatorFactory = new MetricAccumulatorFactory(
                                minimumFactory, maximumFactory, averageFactory, sumFactory);

                        // Aggregate metrics
                        Collection<AggregatedMetric> aggregatedMetrics = new ArrayList<>(requestedMetrics.size());

                        // Add A4S metrics (vertex-level, not aggregated)
                        for (String requestedMetric : requestedMetrics) {
                            if (isA4SVertexLevelMetric(requestedMetric)) {
                                AggregatedMetric metric = getA4SVertexLevelMetric(requestedMetric, jobVertex);
                                if (metric != null) {
                                    aggregatedMetrics.add(metric);
                                }
                                continue;
                            }

                            // For regular metrics, check if we need to add A4S values
                            final Collection<Double> values = new ArrayList<>(stores.size());
                            try {
                                for (MetricStore.ComponentMetricStore storeItem : stores) {
                                    String stringValue = storeItem.metrics.get(requestedMetric);
                                    // If metric not found and it's an A4S metric, add it from vertex info
                                    if (stringValue == null && isA4SMetric(requestedMetric)) {
                                        stringValue = getA4SMetricValueFromVertex(requestedMetric, jobVertex);
                                    }
                                    if (stringValue != null) {
                                        values.add(Double.valueOf(stringValue));
                                    }
                                }
                            } catch (NumberFormatException nfe) {
                                log.warn("The metric {} is not numeric and can't be aggregated.", requestedMetric, nfe);
                                continue;
                            }
                            if (!values.isEmpty()) {
                                Iterator<Double> valuesIterator = values.iterator();
                                MetricAccumulator acc = metricAccumulatorFactory.get(requestedMetric,
                                        valuesIterator.next());
                                valuesIterator.forEachRemaining(acc::add);
                                aggregatedMetrics.add(acc.get());
                            }
                        }

                        return new AggregatedMetricsResponseBody(aggregatedMetrics);
                    } catch (Exception e) {
                        log.warn("Could not retrieve A4S metrics.", e);
                        throw new CompletionException(new RestHandlerException(
                                "Could not retrieve A4S metrics.", HttpResponseStatus.INTERNAL_SERVER_ERROR));
                    }
                }, this.executor));
    }

    private boolean isA4SVertexLevelMetric(String metricName) {
        return metricName.equals(MetricNames.VERTEX_PARALLELISM)
                || metricName.equals(MetricNames.VERTEX_MAX_PARALLELISM);
    }

    private boolean isA4SMetric(String metricName) {
        return isA4SVertexLevelMetric(metricName)
                || metricName.startsWith("resourceProfile.")
                || metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_TASK_HEAP_MEMORY)
                || metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_TASK_OFF_HEAP_MEMORY)
                || metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_MANAGED_MEMORY)
                || metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_NETWORK_MEMORY)
                || metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_TOTAL_MEMORY)
                || metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_OPERATORS_MEMORY);
    }

    @Nullable
    private AggregatedMetric getA4SVertexLevelMetric(String metricName, AccessExecutionJobVertex jobVertex) {
        Double v;
        if (metricName.equals(MetricNames.VERTEX_PARALLELISM)) {
            v = Double.valueOf(jobVertex.getParallelism());
        } else if (metricName.equals(MetricNames.VERTEX_MAX_PARALLELISM)) {
            v = Double.valueOf(jobVertex.getMaxParallelism());
        } else {
            return null;
        }
        return new AggregatedMetric(metricName, v, v, v, v);
    }

    @Nullable
    private String getA4SMetricValueFromVertex(String metricName, AccessExecutionJobVertex jobVertex) {
        ResourceProfile resourceProfile = jobVertex.getResourceProfile();
        if (resourceProfile == null || resourceProfile.equals(ResourceProfile.UNKNOWN)) {
            return null;
        }

        if (metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_TASK_HEAP_MEMORY)) {
            return String.valueOf(resourceProfile.getTaskHeapMemory().getBytes());
        } else if (metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_TASK_OFF_HEAP_MEMORY)) {
            return String.valueOf(resourceProfile.getTaskOffHeapMemory().getBytes());
        } else if (metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_MANAGED_MEMORY)) {
            return String.valueOf(resourceProfile.getManagedMemory().getBytes());
        } else if (metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_NETWORK_MEMORY)) {
            return String.valueOf(resourceProfile.getNetworkMemory().getBytes());
        } else if (metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_TOTAL_MEMORY)) {
            return String.valueOf(resourceProfile.getTotalMemory().getBytes());
        } else if (metricName.equals(MetricNames.VERTEX_RESOURCE_PROFILE_OPERATORS_MEMORY)) {
            return String.valueOf(resourceProfile.getOperatorsMemory().getBytes());
        }

        return null;
    }

    private Collection<String> getA4SMetricNames() {
        List<String> a4sMetrics = new ArrayList<>();
        a4sMetrics.add(MetricNames.VERTEX_PARALLELISM);
        a4sMetrics.add(MetricNames.VERTEX_MAX_PARALLELISM);
        a4sMetrics.add(MetricNames.VERTEX_RESOURCE_PROFILE_TASK_HEAP_MEMORY);
        a4sMetrics.add(MetricNames.VERTEX_RESOURCE_PROFILE_TASK_OFF_HEAP_MEMORY);
        a4sMetrics.add(MetricNames.VERTEX_RESOURCE_PROFILE_MANAGED_MEMORY);
        a4sMetrics.add(MetricNames.VERTEX_RESOURCE_PROFILE_NETWORK_MEMORY);
        a4sMetrics.add(MetricNames.VERTEX_RESOURCE_PROFILE_TOTAL_MEMORY);
        a4sMetrics.add(MetricNames.VERTEX_RESOURCE_PROFILE_OPERATORS_MEMORY);
        a4sMetrics.add(MetricNames.TASK_MEMORY_HEAP_USED);
        a4sMetrics.add(MetricNames.TASK_MEMORY_MANAGED_USED);
        a4sMetrics.add(MetricNames.TASK_MEMORY_TOTAL_USED);
        return a4sMetrics;
    }

    // Helper classes (copied from AbstractAggregatingMetricsHandler for metric
    // accumulation)
    private static class MetricAccumulatorFactory {
        @Nullable
        private final DoubleAccumulator.DoubleMinimumFactory minimumFactory;

        @Nullable
        private final DoubleAccumulator.DoubleMaximumFactory maximumFactory;

        @Nullable
        private final DoubleAccumulator.DoubleAverageFactory averageFactory;

        @Nullable
        private final DoubleAccumulator.DoubleSumFactory sumFactory;

        private MetricAccumulatorFactory(@Nullable DoubleAccumulator.DoubleMinimumFactory minimumFactory,
                @Nullable DoubleAccumulator.DoubleMaximumFactory maximumFactory,
                @Nullable DoubleAccumulator.DoubleAverageFactory averageFactory,
                @Nullable DoubleAccumulator.DoubleSumFactory sumFactory) {
            this.minimumFactory = minimumFactory;
            this.maximumFactory = maximumFactory;
            this.averageFactory = averageFactory;
            this.sumFactory = sumFactory;
        }

        MetricAccumulator get(String metricName, double init) {
            return new MetricAccumulator(metricName,
                    minimumFactory == null ? null : minimumFactory.get(init),
                    maximumFactory == null ? null : maximumFactory.get(init),
                    averageFactory == null ? null : averageFactory.get(init),
                    sumFactory == null ? null : sumFactory.get(init));
        }
    }

    private static class MetricAccumulator {
        private final String metricName;

        @Nullable
        private final DoubleAccumulator min;
        @Nullable
        private final DoubleAccumulator max;
        @Nullable
        private final DoubleAccumulator avg;
        @Nullable
        private final DoubleAccumulator sum;

        private MetricAccumulator(String metricName, @Nullable DoubleAccumulator min,
                @Nullable DoubleAccumulator max, @Nullable DoubleAccumulator avg,
                @Nullable DoubleAccumulator sum) {
            this.metricName = Preconditions.checkNotNull(metricName);
            this.min = min;
            this.max = max;
            this.avg = avg;
            this.sum = sum;
        }

        void add(double value) {
            if (min != null) {
                min.add(value);
            }
            if (max != null) {
                max.add(value);
            }
            if (avg != null) {
                avg.add(value);
            }
            if (sum != null) {
                sum.add(value);
            }
        }

        AggregatedMetric get() {
            return new AggregatedMetric(metricName,
                    min == null ? null : min.getValue(),
                    max == null ? null : max.getValue(),
                    avg == null ? null : avg.getValue(),
                    sum == null ? null : sum.getValue());
        }
    }
}
