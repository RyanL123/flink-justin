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
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedVertexMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.standalone_stackhistogram.QuickMRC;
import org.apache.flink.runtime.standalone_stackhistogram.StackHistogram;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;

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
        extends
        AbstractRestHandler<RestfulGateway, EmptyRequestBody, A4SAggregatedMetricsResponseBody, AggregatedSubtaskMetricsParameters> {

    private static final String STACK_DISTANCE_HISTOGRAM_METRIC_NAME = "stack-distance-histogram";
    private static final String CURVE_LOG_PREFIX = "A4S_CURVE";
    private static final String TRACE_LOG_PREFIX = "A4S_TRACE";

    private final Executor executor;
    private final MetricFetcher fetcher;
    private final ExecutionGraphCache executionGraphCache;

    private final long cacheItemSizeBytes;
    private final long bucketSizeScaling;

    public A4SAggregatingVertexMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> responseHeaders,
            Executor executor,
            MetricFetcher fetcher,
            ExecutionGraphCache executionGraphCache,
            long cacheItemSizeBytes,
            long bucketSizeScaling) {
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                A4SAggregatedVertexMetricsHeaders.getInstance());
        this.executor = executor;
        this.fetcher = fetcher;
        this.executionGraphCache = executionGraphCache;
        this.cacheItemSizeBytes = cacheItemSizeBytes;
        this.bucketSizeScaling = bucketSizeScaling;
    }

    @Override
    protected CompletableFuture<A4SAggregatedMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request,
            @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID vertexID = request.getPathParameter(JobVertexIdPathParameter.class);
        long requestStartEpochMs = System.currentTimeMillis();

        log.info(
                "{} stage=request_received jobId={} vertexId={} timestampEpochMs={} requestedMetrics={} requestedAggregations={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                requestStartEpochMs);

        return executionGraphCache.getExecutionGraphInfo(jobId, gateway)
                .thenCompose(executionGraphInfo -> CompletableFuture.supplyAsync(() -> {
                    try {
                        return processRequest(
                                jobId,
                                vertexID,
                                executionGraphInfo);
                    } catch (Exception e) {
                        log.warn(
                                "{} stage=handler_failed jobId={} vertexId={} elapsedMs={} message={}",
                                TRACE_LOG_PREFIX,
                                jobId,
                                vertexID,
                                (System.currentTimeMillis() - requestStartEpochMs),
                                e.getMessage(),
                                e);
                        throw new CompletionException(new RestHandlerException(
                                "Could not retrieve A4S metrics.", HttpResponseStatus.INTERNAL_SERVER_ERROR));
                    }
                }, this.executor));
    }

    @Nonnull
    private Collection<MetricStore.SubtaskMetricStore> getStores(
            MetricStore store, JobID jobID, JobVertexID taskID) {
        MetricStore.TaskMetricStore taskMetricStore = store.getTaskMetricStore(jobID.toString(), taskID.toString());
        if (taskMetricStore == null) {
            log.info(
                    "{} stage=stores_missing jobId={} vertexId={} reason=task_metric_store_not_found",
                    TRACE_LOG_PREFIX,
                    jobID,
                    taskID);
            return Collections.emptyList();
        }
        log.debug(
                "{} stage=stores_found jobId={} vertexId={} subtaskStoreCount={}",
                TRACE_LOG_PREFIX,
                jobID,
                taskID,
                taskMetricStore.getAllSubtaskMetricStores().size());
        return taskMetricStore.getAllSubtaskMetricStores().values();
    }

    private A4SAggregatedMetricsResponseBody processRequest(
            JobID jobId,
            JobVertexID vertexID,
            ExecutionGraphInfo executionGraphInfo) throws Exception {
        // === Stage 1: Query and update metrics from TMs ===
        long requestStartEpochMs = System.currentTimeMillis();
        log.info(
                "{} stage=fetcher_update_begin jobId={} vertexId={} timestampEpochMs={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                requestStartEpochMs);
        this.fetcher.update();
        MetricStore store = this.fetcher.getMetricStore();
        log.info(
                "{} stage=fetcher_update_end jobId={} vertexId={} timestampEpochMs={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                System.currentTimeMillis());

        // Get vertex information from ExecutionGraph
        AccessExecutionGraph executionGraph = executionGraphInfo.getArchivedExecutionGraph();
        AccessExecutionJobVertex jobVertex = executionGraph.getJobVertex(vertexID);

        if (jobVertex == null) {
            log.warn(
                    "{} stage=vertex_not_found jobId={} vertexId={} reason=missing_in_execution_graph",
                    TRACE_LOG_PREFIX,
                    jobId,
                    vertexID);
            throw new CompletionException(new RestHandlerException(
                    String.format("JobVertex %s not found", vertexID),
                    HttpResponseStatus.NOT_FOUND));
        }

        // === Stage 2: Fetch and deserialize histograms ===
        Collection<MetricStore.SubtaskMetricStore> stores = getStores(store, jobId, vertexID);
        log.info(
                "{} stage=store_scan_begin jobId={} vertexId={} subtaskStoreCount={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                stores.size());

        List<StackHistogram> subtaskHistograms = new ArrayList<>(stores.size());
        for (MetricStore.SubtaskMetricStore storeItem : stores) {
            log.info(
                    "{} stage=store_item_begin jobId={} vertexId={} metricCount={}",
                    TRACE_LOG_PREFIX,
                    jobId,
                    vertexID,
                    storeItem.metrics.size());
            String histogramRaw = getStackDistanceHistogramRaw(storeItem.metrics);
            if (histogramRaw == null) {
                continue;
            }
            StackHistogram histogram = StackHistogram.fromSerializedValue(histogramRaw);
            subtaskHistograms.add(histogram);
        }
        log.info(
                "{} stage=store_scan_end jobId={} vertexId={} scannedSources={} parsedHistograms={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                stores.size(),
                subtaskHistograms.size());

        // === Stage 3: Build scaled MRC points ===
        List<QuickMRC.MRCPoint> scaledMrcPoints = buildScaledMrcPoints(
                jobId,
                vertexID,
                subtaskHistograms
        );
        log.info(
                "{} stage=response_ready jobId={} vertexId={} scaledMrcPointCount={} elapsedMs={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                scaledMrcPoints.size(),
                (System.currentTimeMillis() - requestStartEpochMs));
        return new A4SAggregatedMetricsResponseBody(scaledMrcPoints);
    }

    @Nullable
    private String getStackDistanceHistogramRaw(Map<String, String> metrics) {
        for (Map.Entry<String, String> metricEntry : metrics.entrySet()) {
            String metricKey = metricEntry.getKey();
            String metricValue = metricEntry.getValue();
            if (metricKey == null || metricValue == null) {
                continue;
            }
            if (metricKey.contains(STACK_DISTANCE_HISTOGRAM_METRIC_NAME)) {
                return metricValue;
            }
        }
        return null;
    }

    private List<QuickMRC.MRCPoint> buildScaledMrcPoints(
            JobID jobId, JobVertexID vertexID, List<StackHistogram> subtaskHistograms) {
        log.info(
                "{} stage=jm_scaled_mrc_begin jobId={} vertexId={} histogramCount={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                subtaskHistograms.size());
        if (subtaskHistograms.isEmpty()) {
            log.info(
                    "{} stage=jm_scaled_mrc jobId={} vertexId={} curveType=scaled_mrc numPoints=0 pointsJson=[]",
                    CURVE_LOG_PREFIX,
                    jobId,
                    vertexID);
            return Collections.emptyList();
        }

        StackHistogram mergedHistogram = StackHistogram.merge(subtaskHistograms);
        log.info(
                "{} stage=jm_histogram_merged jobId={} vertexId={} numBuckets={} totalFrequency={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                mergedHistogram.getNumBuckets(),
                mergedHistogram.getTotalFrequency());
        List<QuickMRC.MRCPoint> mrc = QuickMRC.computeScaledMRC(mergedHistogram, cacheItemSizeBytes, bucketSizeScaling);
        log.info(
                "{} stage=jm_scaled_mrc jobId={} vertexId={} curveType=scaled_mrc numPoints={} pointsJson={}",
                CURVE_LOG_PREFIX,
                jobId,
                vertexID,
                mrc.size(),
                mrc);
        return mrc;
    }
}
