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
import org.apache.flink.runtime.rest.handler.AbstractRestHandler;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.ExecutionGraphCache;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedMetricsResponseBody;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedMetric;
import org.apache.flink.runtime.rest.messages.job.metrics.A4SAggregatedVertexMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.AggregatedSubtaskMetricsParameters;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsAggregationParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.MetricsFilterParameter;
import org.apache.flink.runtime.standalone_stackhistogram.QuickMRC;
import org.apache.flink.runtime.standalone_stackhistogram.StackHistogram;
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
import java.util.HashSet;
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
        extends AbstractRestHandler<
                RestfulGateway,
                EmptyRequestBody,
                A4SAggregatedMetricsResponseBody,
                AggregatedSubtaskMetricsParameters> {

    private static final String STACK_DISTANCE_HISTOGRAM_METRIC_NAME =
            "rocksdb.stack-distance-histogram";
    private static final String STACK_DISTANCE_HISTOGRAM_METRIC_NAME_ALT =
            "rocksdb.stack_distance_histogram";
    private static final String CURVE_LOG_PREFIX = "A4S_CURVE";
    private static final String TRACE_LOG_PREFIX = "A4S_TRACE";
    private static final String STORE_LOG_PREFIX = "A4S_METRIC_STORE";

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
        super(
                leaderRetriever,
                timeout,
                responseHeaders,
                A4SAggregatedVertexMetricsHeaders.getInstance());
        this.executor = executor;
        this.fetcher = fetcher;
        this.executionGraphCache = executionGraphCache;
    }

    @Nonnull
    private Collection<MetricStore.SubtaskMetricStore> getStores(
            MetricStore store, HandlerRequest<EmptyRequestBody> request) {
        JobID jobID = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID taskID = request.getPathParameter(JobVertexIdPathParameter.class);

        MetricStore.TaskMetricStore taskMetricStore = store.getTaskMetricStore(jobID.toString(), taskID.toString());
        if (taskMetricStore != null) {
            log.debug(
                    "{} stage=stores_found jobId={} vertexId={} subtaskStoreCount={}",
                    TRACE_LOG_PREFIX,
                    jobID,
                    taskID,
                    taskMetricStore.getAllSubtaskMetricStores().size());
            return taskMetricStore.getAllSubtaskMetricStores().values();
        } else {
            log.info(
                    "{} stage=stores_missing jobId={} vertexId={} reason=task_metric_store_not_found",
                    TRACE_LOG_PREFIX,
                    jobID,
                    taskID);
            return Collections.emptyList();
        }
    }

    @Override
    protected CompletableFuture<A4SAggregatedMetricsResponseBody> handleRequest(
            @Nonnull HandlerRequest<EmptyRequestBody> request, @Nonnull RestfulGateway gateway)
            throws RestHandlerException {
        JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        JobVertexID vertexID = request.getPathParameter(JobVertexIdPathParameter.class);
        List<String> requestedMetrics = request.getQueryParameter(MetricsFilterParameter.class);
        List<MetricsAggregationParameter.AggregationMode> requestedAggregations =
                request.getQueryParameter(MetricsAggregationParameter.class);
        long requestStartEpochMs = System.currentTimeMillis();

        log.info(
                "{} stage=request_received jobId={} vertexId={} timestampEpochMs={} requestedMetrics={} requestedAggregations={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                requestStartEpochMs,
                requestedMetrics,
                requestedAggregations);

        return executionGraphCache.getExecutionGraphInfo(jobId, gateway)
                .thenCompose(executionGraphInfo -> CompletableFuture.supplyAsync(() -> {
                    try {
                        log.info(
                                "{} stage=fetcher_update_begin jobId={} vertexId={} timestampEpochMs={}",
                                TRACE_LOG_PREFIX,
                                jobId,
                                vertexID,
                                System.currentTimeMillis());
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

                        // A4S metrics will be added on-the-fly during aggregation

                        // Now use parent's handleRequest logic
                        // We need to call the parent's implementation
                        // but since it's not accessible, we'll implement
                        // the logic here
                        Collection<MetricStore.SubtaskMetricStore> stores =
                                getStores(store, request);
                        log.info(
                                "{} stage=store_scan_begin jobId={} vertexId={} subtaskStoreCount={}",
                                TRACE_LOG_PREFIX,
                                jobId,
                                vertexID,
                                stores.size());

                        List<StackHistogram> subtaskHistograms = new ArrayList<>(stores.size());
                        int histogramSourceIndex = 0;
                        for (MetricStore.SubtaskMetricStore storeItem : stores) {
                            List<HistogramCandidate> histogramCandidates = new ArrayList<>();
                            logMetricStoreEntries(
                                    jobId,
                                    vertexID,
                                    histogramSourceIndex,
                                    "subtask",
                                    null,
                                    storeItem.metrics);
                            HistogramMetricMatch subtaskMatch =
                                    getStackDistanceHistogramValue(storeItem.metrics);
                            if (subtaskMatch != null) {
                                histogramCandidates.add(
                                        new HistogramCandidate(
                                                subtaskMatch, "subtask", null, subtaskMatch.value));
                                log.debug(
                                        "{} stage=tm_histogram_found jobId={} vertexId={} sourceIndex={} metricKey={} matchMode={}",
                                        TRACE_LOG_PREFIX,
                                        jobId,
                                        vertexID,
                                        histogramSourceIndex,
                                        subtaskMatch.metricKey,
                                        subtaskMatch.matchMode);
                            } else {
                                log.debug(
                                        "{} stage=tm_histogram_missing jobId={} vertexId={} sourceIndex={} scope=subtask metricEntryCount={} candidateKeys={}",
                                        TRACE_LOG_PREFIX,
                                        jobId,
                                        vertexID,
                                        histogramSourceIndex,
                                        storeItem.metrics.size(),
                                        summarizeCandidateKeys(storeItem.metrics));
                            }

                            int attemptStoreCount = storeItem.getAllAttemptsMetricStores().size();
                            int attemptMatchCount = 0;
                            for (Map.Entry<Integer, MetricStore.ComponentMetricStore> attemptEntry :
                                    storeItem.getAllAttemptsMetricStores().entrySet()) {
                                Integer attemptNumber = attemptEntry.getKey();
                                Map<String, String> attemptMetrics = attemptEntry.getValue().metrics;
                                logMetricStoreEntries(
                                        jobId,
                                        vertexID,
                                        histogramSourceIndex,
                                        "attempt",
                                        attemptNumber,
                                        attemptMetrics);
                                HistogramMetricMatch attemptMatch =
                                        getStackDistanceHistogramValue(attemptMetrics);
                                if (attemptMatch != null) {
                                    attemptMatchCount++;
                                    histogramCandidates.add(
                                            new HistogramCandidate(
                                                    attemptMatch, "attempt", attemptNumber, attemptMatch.value));
                                    log.debug(
                                            "{} stage=tm_histogram_found jobId={} vertexId={} sourceIndex={} scope=attempt attemptNumber={} metricKey={} matchMode={}",
                                            TRACE_LOG_PREFIX,
                                            jobId,
                                            vertexID,
                                            histogramSourceIndex,
                                            attemptNumber,
                                            attemptMatch.metricKey,
                                            attemptMatch.matchMode);
                                } else {
                                    log.debug(
                                            "{} stage=tm_histogram_missing jobId={} vertexId={} sourceIndex={} scope=attempt attemptNumber={} metricEntryCount={} candidateKeys={}",
                                            TRACE_LOG_PREFIX,
                                            jobId,
                                            vertexID,
                                            histogramSourceIndex,
                                            attemptNumber,
                                            attemptMetrics.size(),
                                            summarizeCandidateKeys(attemptMetrics));
                                }
                            }
                            log.info(
                                    "{} stage=attempt_histogram_scan_summary jobId={} vertexId={} sourceIndex={} attemptStoreCount={} attemptMatchCount={} totalCandidateCount={}",
                                    TRACE_LOG_PREFIX,
                                    jobId,
                                    vertexID,
                                    histogramSourceIndex,
                                    attemptStoreCount,
                                    attemptMatchCount,
                                    histogramCandidates.size());

                            if (histogramCandidates.isEmpty()) {
                                histogramSourceIndex++;
                                continue;
                            }

                            Set<String> seenSerializedHistograms = new HashSet<>();
                            for (HistogramCandidate candidate : histogramCandidates) {
                                if (!seenSerializedHistograms.add(candidate.serializedValue)) {
                                    log.debug(
                                            "{} stage=tm_histogram_duplicate_skipped jobId={} vertexId={} sourceIndex={} scope={} attemptNumber={} metricKey={}",
                                            TRACE_LOG_PREFIX,
                                            jobId,
                                            vertexID,
                                            histogramSourceIndex,
                                            candidate.scope,
                                            candidate.attemptNumber,
                                            candidate.match.metricKey);
                                    continue;
                                }

                                String serializedHistogram = candidate.serializedValue;
                                log.debug(
                                        "{} stage=tm_histogram_selected jobId={} vertexId={} sourceIndex={} scope={} attemptNumber={} metricKey={} matchMode={} serializedLength={}",
                                        TRACE_LOG_PREFIX,
                                        jobId,
                                        vertexID,
                                        histogramSourceIndex,
                                        candidate.scope,
                                        candidate.attemptNumber,
                                        candidate.match.metricKey,
                                        candidate.match.matchMode,
                                        serializedHistogram.length());
                                try {
                                    StackHistogram histogram =
                                            StackHistogram.fromSerializedValue(serializedHistogram);
                                    subtaskHistograms.add(histogram);
                                    logHistogramAndUnscaledMrc(
                                            jobId,
                                            vertexID,
                                            histogramSourceIndex,
                                            histogram,
                                            serializedHistogram);
                                } catch (IllegalArgumentException histogramParseException) {
                                    log.warn(
                                            "Unable to parse stack histogram for job {}, vertex {}, source {}, scope {}, attempt {}",
                                            jobId,
                                            vertexID,
                                            histogramSourceIndex,
                                            candidate.scope,
                                            candidate.attemptNumber,
                                            histogramParseException);
                                }
                            }
                            histogramSourceIndex++;
                        }
                        log.info(
                                "{} stage=store_scan_end jobId={} vertexId={} scannedSources={} parsedHistograms={}",
                                TRACE_LOG_PREFIX,
                                jobId,
                                vertexID,
                                histogramSourceIndex,
                                subtaskHistograms.size());

                        List<A4SAggregatedMetricsResponseBody.MRCPoint> scaledMrcPoints =
                                buildScaledMrcPoints(jobId, vertexID, subtaskHistograms);
                        log.info(
                                "{} stage=scaled_mrc_ready jobId={} vertexId={} scaledMrcPointCount={}",
                                TRACE_LOG_PREFIX,
                                jobId,
                                vertexID,
                                scaledMrcPoints.size());

                        if (requestedMetrics.isEmpty()) {
                            Set<String> uniqueMetrics = CollectionUtil.newHashSetWithExpectedSize(32);
                                for (MetricStore.SubtaskMetricStore storeItem : stores) {
                                uniqueMetrics.addAll(storeItem.metrics.keySet());
                            }
                            // Add A4S-specific metrics to the list
                            uniqueMetrics.addAll(getA4SMetricNames());
                            log.info(
                                    "{} stage=response_metric_catalog jobId={} vertexId={} metricCount={} scaledMrcPointCount={}",
                                    TRACE_LOG_PREFIX,
                                    jobId,
                                    vertexID,
                                    uniqueMetrics.size(),
                                    scaledMrcPoints.size());
                            return new A4SAggregatedMetricsResponseBody(
                                    uniqueMetrics.stream()
                                            .map(AggregatedMetric::new)
                                            .collect(Collectors.toList()),
                                    scaledMrcPoints);
                        }

                        // Create accumulator factories
                        DoubleAccumulator.DoubleMinimumFactory minimumFactory = null;
                        DoubleAccumulator.DoubleMaximumFactory maximumFactory = null;
                        DoubleAccumulator.DoubleAverageFactory averageFactory = null;
                        DoubleAccumulator.DoubleSumFactory sumFactory = null;

                        if (requestedAggregations.isEmpty()) {
                            log.info(
                                    "{} stage=aggregation_mode jobId={} vertexId={} mode=default_all",
                                    TRACE_LOG_PREFIX,
                                    jobId,
                                    vertexID);
                            minimumFactory = DoubleAccumulator.DoubleMinimumFactory.get();
                            maximumFactory = DoubleAccumulator.DoubleMaximumFactory.get();
                            averageFactory = DoubleAccumulator.DoubleAverageFactory.get();
                            sumFactory = DoubleAccumulator.DoubleSumFactory.get();
                        } else {
                            log.info(
                                    "{} stage=aggregation_mode jobId={} vertexId={} mode=requested requestedAggregations={}",
                                    TRACE_LOG_PREFIX,
                                    jobId,
                                    vertexID,
                                    requestedAggregations);
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
                                    log.info(
                                            "{} stage=metric_vertex_level jobId={} vertexId={} metricName={} value={}",
                                            TRACE_LOG_PREFIX,
                                            jobId,
                                            vertexID,
                                            requestedMetric,
                                            metric.getSum());
                                } else {
                                    log.warn(
                                            "{} stage=metric_vertex_level_missing jobId={} vertexId={} metricName={}",
                                            TRACE_LOG_PREFIX,
                                            jobId,
                                            vertexID,
                                            requestedMetric);
                                }
                                continue;
                            }

                            // For regular metrics, check if we need to add A4S values
                            final Collection<Double> values = new ArrayList<>(stores.size());
                            try {
                                for (MetricStore.SubtaskMetricStore storeItem : stores) {
                                    String stringValue = storeItem.metrics.get(requestedMetric);
                                    // If metric not found and it's an A4S metric, add it from vertex info
                                    if (stringValue == null && isA4SMetric(requestedMetric)) {
                                        stringValue = getA4SMetricValueFromVertex(requestedMetric, jobVertex);
                                        if (stringValue != null) {
                                            log.debug(
                                                    "{} stage=metric_fallback_vertex_value jobId={} vertexId={} metricName={} value={}",
                                                    TRACE_LOG_PREFIX,
                                                    jobId,
                                                    vertexID,
                                                    requestedMetric,
                                                    stringValue);
                                        }
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
                                AggregatedMetric aggregatedMetric = acc.get();
                                aggregatedMetrics.add(aggregatedMetric);
                                log.info(
                                        "{} stage=metric_aggregated jobId={} vertexId={} metricName={} valueCount={} min={} max={} avg={} sum={}",
                                        TRACE_LOG_PREFIX,
                                        jobId,
                                        vertexID,
                                        requestedMetric,
                                        values.size(),
                                        aggregatedMetric.getMin(),
                                        aggregatedMetric.getMax(),
                                        aggregatedMetric.getAvg(),
                                        aggregatedMetric.getSum());
                            } else {
                                log.info(
                                        "{} stage=metric_no_values jobId={} vertexId={} metricName={}",
                                        TRACE_LOG_PREFIX,
                                        jobId,
                                        vertexID,
                                        requestedMetric);
                            }
                        }

                        log.info(
                                "{} stage=response_ready jobId={} vertexId={} aggregatedMetricCount={} scaledMrcPointCount={} elapsedMs={}",
                                TRACE_LOG_PREFIX,
                                jobId,
                                vertexID,
                                aggregatedMetrics.size(),
                                scaledMrcPoints.size(),
                                (System.currentTimeMillis() - requestStartEpochMs));
                        return new A4SAggregatedMetricsResponseBody(
                                aggregatedMetrics, scaledMrcPoints);
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

    @Nullable
    private static HistogramMetricMatch getStackDistanceHistogramValue(Map<String, String> metrics) {
        Map.Entry<String, String> firstJsonLikeCandidate = null;
        for (Map.Entry<String, String> metricEntry : metrics.entrySet()) {
            String metricKey = metricEntry.getKey();
            String metricValue = metricEntry.getValue();
            if (metricKey == null || metricValue == null) {
                continue;
            }

            if (isStackDistanceHistogramMetricName(metricKey)) {
                return new HistogramMetricMatch(metricKey, metricValue, "name_match");
            }
            // Fallback for future naming variations where key shape changes but value still carries
            // the SD histogram JSON payload.
            if (looksLikeStackDistanceHistogramPayload(metricValue) && firstJsonLikeCandidate == null) {
                firstJsonLikeCandidate = metricEntry;
            }
        }
        if (firstJsonLikeCandidate != null) {
            return new HistogramMetricMatch(
                    firstJsonLikeCandidate.getKey(), firstJsonLikeCandidate.getValue(), "json_payload_fallback");
        }
        return null;
    }

    private static boolean isStackDistanceHistogramMetricName(String metricName) {
        return metricName.endsWith(STACK_DISTANCE_HISTOGRAM_METRIC_NAME)
                || metricName.endsWith(STACK_DISTANCE_HISTOGRAM_METRIC_NAME_ALT)
                || metricName.contains(STACK_DISTANCE_HISTOGRAM_METRIC_NAME)
                || metricName.contains(STACK_DISTANCE_HISTOGRAM_METRIC_NAME_ALT)
                || metricName.contains("stack-distance-histogram")
                || metricName.contains("stack_distance_histogram");
    }

    private static boolean looksLikeStackDistanceHistogramPayload(String payload) {
        return payload.startsWith("{\"boundaries\":[") && payload.contains("],\"counts\":[");
    }

    private static String summarizeCandidateKeys(Map<String, String> metrics) {
        return metrics.keySet().stream()
                .filter(
                        key ->
                                key != null
                                        && (key.contains("stack")
                                                || key.contains("rocksdb")
                                                || key.contains("histogram")))
                .limit(12)
                .collect(Collectors.joining(","));
    }

    private void logMetricStoreEntries(
            JobID jobId,
            JobVertexID vertexID,
            int sourceIndex,
            String scope,
            @Nullable Integer attemptNumber,
            Map<String, String> metrics) {
        if (!log.isInfoEnabled()) {
            return;
        }

        log.info(
                "{} stage=metric_store_dump jobId={} vertexId={} sourceIndex={} scope={} attemptNumber={} entryCount={} entriesJson={}",
                STORE_LOG_PREFIX,
                jobId,
                vertexID,
                sourceIndex,
                scope,
                attemptNumber,
                metrics.size(),
                toMetricsJson(metrics));
    }

    private static String toMetricsJson(Map<String, String> metrics) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        List<Map.Entry<String, String>> sortedEntries =
                metrics.entrySet().stream()
                        .sorted(Map.Entry.comparingByKey())
                        .collect(Collectors.toList());
        for (int i = 0; i < sortedEntries.size(); i++) {
            Map.Entry<String, String> entry = sortedEntries.get(i);
            if (i > 0) {
                sb.append(',');
            }
            sb.append('"')
                    .append(escapeJson(entry.getKey()))
                    .append("\":\"")
                    .append(escapeJson(entry.getValue()))
                    .append('"');
        }
        sb.append('}');
        return sb.toString();
    }

    private static String escapeJson(String value) {
        if (value == null) {
            return "null";
        }
        StringBuilder out = new StringBuilder(value.length() + 16);
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\\':
                    out.append("\\\\");
                    break;
                case '"':
                    out.append("\\\"");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    out.append(ch);
            }
        }
        return out.toString();
    }

    private List<A4SAggregatedMetricsResponseBody.MRCPoint> buildScaledMrcPoints(
            JobID jobId, JobVertexID vertexID, List<StackHistogram> subtaskHistograms) {
        log.info(
                "{} stage=jm_scaled_mrc_begin jobId={} vertexId={} histogramCount={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                subtaskHistograms.size());
        if (subtaskHistograms.isEmpty()) {
            log.info(
                    "{} stage=jm_scaled_mrc jobId={} vertexId={} timestampEpochMs={} curveType=scaled_mrc numPoints=0 pointsJson=[]",
                    CURVE_LOG_PREFIX,
                    jobId,
                    vertexID,
                    System.currentTimeMillis());
            return Collections.emptyList();
        }

        StackHistogram mergedHistogram = StackHistogram.merge(subtaskHistograms);
        log.info(
                "{} stage=jm_histogram_merged jobId={} vertexId={} numBuckets={} maxStackDistance={} totalFrequency={}",
                TRACE_LOG_PREFIX,
                jobId,
                vertexID,
                mergedHistogram.getNumBuckets(),
                mergedHistogram.getMaxStackDistance(),
                mergedHistogram.getTotalFrequency());
        List<QuickMRC.MRCPoint> scaledMrc = QuickMRC.computeScaledMRC(mergedHistogram);
        log.info(
                "{} stage=jm_scaled_mrc jobId={} vertexId={} timestampEpochMs={} curveType=scaled_mrc numPoints={} pointsJson={}",
                CURVE_LOG_PREFIX,
                jobId,
                vertexID,
                System.currentTimeMillis(),
                scaledMrc.size(),
                toQuickMrcPointsJson(scaledMrc));

        return scaledMrc.stream()
                .map(
                        p ->
                                new A4SAggregatedMetricsResponseBody.MRCPoint(
                                        p.getCacheSize(), p.getMissRate()))
                .collect(Collectors.toList());
    }

    private void logHistogramAndUnscaledMrc(
            JobID jobId,
            JobVertexID vertexID,
            int sourceIndex,
            StackHistogram histogram,
            String serializedHistogram) {
        log.info(
                "{} stage=tm_histogram jobId={} vertexId={} timestampEpochMs={} sourceIndex={} curveType=stack_histogram numBuckets={} maxStackDistance={} totalFrequency={} serializedHistogram={}",
                CURVE_LOG_PREFIX,
                jobId,
                vertexID,
                System.currentTimeMillis(),
                sourceIndex,
                histogram.getNumBuckets(),
                histogram.getMaxStackDistance(),
                histogram.getTotalFrequency(),
                serializedHistogram);

        List<QuickMRC.MRCPoint> unscaledMrc = QuickMRC.computeUnscaledMRC(histogram);
        log.info(
                "{} stage=tm_unscaled_mrc jobId={} vertexId={} timestampEpochMs={} sourceIndex={} curveType=unscaled_mrc numPoints={} pointsJson={}",
                CURVE_LOG_PREFIX,
                jobId,
                vertexID,
                System.currentTimeMillis(),
                sourceIndex,
                unscaledMrc.size(),
                toQuickMrcPointsJson(unscaledMrc));
    }

    private static String toQuickMrcPointsJson(List<QuickMRC.MRCPoint> points) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (int i = 0; i < points.size(); i++) {
            QuickMRC.MRCPoint point = points.get(i);
            if (i > 0) {
                sb.append(',');
            }
            sb.append("{\"cacheSizeBytes\":")
                    .append(point.getCacheSize())
                    .append(",\"missRate\":")
                    .append(point.getMissRate())
                    .append('}');
        }
        sb.append(']');
        return sb.toString();
    }

    private static class HistogramMetricMatch {
        private final String metricKey;
        private final String value;
        private final String matchMode;

        private HistogramMetricMatch(String metricKey, String value, String matchMode) {
            this.metricKey = metricKey;
            this.value = value;
            this.matchMode = matchMode;
        }
    }

    private static class HistogramCandidate {
        private final HistogramMetricMatch match;
        private final String scope;
        @Nullable private final Integer attemptNumber;
        private final String serializedValue;

        private HistogramCandidate(
                HistogramMetricMatch match,
                String scope,
                @Nullable Integer attemptNumber,
                String serializedValue) {
            this.match = match;
            this.scope = scope;
            this.attemptNumber = attemptNumber;
            this.serializedValue = serializedValue;
        }
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
            log.debug(
                    "{} stage=resource_profile_unavailable vertexId={} metricName={} resourceProfile={}",
                    TRACE_LOG_PREFIX,
                    jobVertex.getJobVertexId(),
                    metricName,
                    resourceProfile);
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

        log.debug(
                "{} stage=resource_profile_metric_unhandled vertexId={} metricName={}",
                TRACE_LOG_PREFIX,
                jobVertex.getJobVertexId(),
                metricName);
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
