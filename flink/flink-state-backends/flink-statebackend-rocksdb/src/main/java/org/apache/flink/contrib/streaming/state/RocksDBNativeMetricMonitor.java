/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.View;
import org.apache.flink.runtime.metrics.dump.StackDistanceHistogramProvider;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Statistics;
import org.rocksdb.TickerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.Closeable;
import java.math.BigInteger;

/**
 * A monitor which pulls {{@link RocksDB}} native metrics and forwards them to Flink's metric group.
 * All metrics are unsigned longs and are reported at the column family level.
 */
@Internal
public class RocksDBNativeMetricMonitor implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(RocksDBNativeMetricMonitor.class);

    private final RocksDBNativeMetricOptions options;

    private final MetricGroup metricGroup;

    private final Object lock;

    static final String COLUMN_FAMILY_KEY = "column_family";

    @GuardedBy("lock")
    private RocksDB rocksDB;

    @Nullable
    @GuardedBy("lock")
    private Statistics statistics;

    public RocksDBNativeMetricMonitor(
            @Nonnull RocksDBNativeMetricOptions options,
            @Nonnull MetricGroup metricGroup,
            @Nonnull RocksDB rocksDB,
            @Nullable Statistics statistics) {
        this.options = options;
        this.metricGroup = metricGroup;
        this.rocksDB = rocksDB;
        this.statistics = statistics;
        this.lock = new Object();
        registerStatistics();
        registerStackDistanceHistogram();
    }

    /** Register gauges to pull native metrics for the database. */
    private void registerStatistics() {
        if (statistics != null) {
            for (TickerType tickerType : options.getMonitorTickerTypes()) {
                metricGroup.gauge(
                        String.format("rocksdb.%s", tickerType.name().toLowerCase()),
                        new RocksDBNativeStatisticsMetricView(tickerType));
            }
        }
    }

    /**
     * Registers the stack distance histogram metric if enabled. The histogram is registered as a
     * Gauge so it flows through the standard metric registration path, but the {@link
     * MetricQueryService} routes it to a dedicated map because it also implements {@link
     * StackDistanceHistogramProvider}.
     */
    private void registerStackDistanceHistogram() {
        if (options.isStackDistanceHistogramEnabled()) {
            LOG.info("Registering stack distance histogram metric for RocksDB.");
            metricGroup.gauge(
                    "rocksdb.stack-distance-histogram",
                    new RocksDBStackDistanceHistogramView());
        }
    }

    /**
     * Register gauges to pull native metrics for the column family.
     *
     * @param columnFamilyName group name for the new gauges
     * @param handle native handle to the column family
     */
    void registerColumnFamily(String columnFamilyName, ColumnFamilyHandle handle) {

        boolean columnFamilyAsVariable = options.isColumnFamilyAsVariable();
        MetricGroup group =
                columnFamilyAsVariable
                        ? metricGroup.addGroup(COLUMN_FAMILY_KEY, columnFamilyName)
                        : metricGroup.addGroup(columnFamilyName);

        for (String property : options.getProperties()) {
            RocksDBNativePropertyMetricView gauge =
                    new RocksDBNativePropertyMetricView(handle, property);
            group.gauge(property, gauge);
        }
    }

    /** Updates the value of metricView if the reference is still valid. */
    private void setProperty(RocksDBNativePropertyMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        try {
            synchronized (lock) {
                if (rocksDB != null) {
                    long value = rocksDB.getLongProperty(metricView.handle, metricView.property);
                    metricView.setValue(value);
                }
            }
        } catch (RocksDBException e) {
            metricView.close();
            LOG.warn("Failed to read native metric {} from RocksDB.", metricView.property, e);
        }
    }

    private void setStatistics(RocksDBNativeStatisticsMetricView metricView) {
        if (metricView.isClosed()) {
            return;
        }
        if (statistics != null) {
            synchronized (lock) {
                metricView.setValue(statistics.getTickerCount(metricView.tickerType));
            }
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            rocksDB = null;
            statistics = null;
        }
    }

    abstract static class RocksDBNativeView implements View {
        private boolean closed;

        RocksDBNativeView() {
            this.closed = false;
        }

        void close() {
            closed = true;
        }

        boolean isClosed() {
            return closed;
        }
    }

    /**
     * A gauge which periodically pulls a RocksDB property-based native metric for the specified
     * column family / metric pair.
     *
     * <p><strong>Note</strong>: As the returned property is of type {@code uint64_t} on C++ side
     * the returning value can be negative. Because java does not support unsigned long types, this
     * gauge wraps the result in a {@link BigInteger}.
     */
    class RocksDBNativePropertyMetricView extends RocksDBNativeView implements Gauge<BigInteger> {
        private final String property;

        private final ColumnFamilyHandle handle;

        private BigInteger bigInteger;

        private RocksDBNativePropertyMetricView(
                ColumnFamilyHandle handle, @Nonnull String property) {
            this.handle = handle;
            this.property = property;
            this.bigInteger = BigInteger.ZERO;
        }

        public void setValue(long value) {
            if (value >= 0L) {
                bigInteger = BigInteger.valueOf(value);
            } else {
                int upper = (int) (value >>> 32);
                int lower = (int) value;

                bigInteger =
                        BigInteger.valueOf(Integer.toUnsignedLong(upper))
                                .shiftLeft(32)
                                .add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
            }
        }

        @Override
        public BigInteger getValue() {
            return bigInteger;
        }

        @Override
        public void update() {
            setProperty(this);
        }
    }

    /**
     * A gauge which periodically pulls a RocksDB statistics-based native metric for the database.
     */
    class RocksDBNativeStatisticsMetricView extends RocksDBNativeView implements Gauge<Long> {
        private final TickerType tickerType;
        private long value;

        private RocksDBNativeStatisticsMetricView(TickerType tickerType) {
            this.tickerType = tickerType;
        }

        @Override
        public Long getValue() {
            return value;
        }

        void setValue(long value) {
            this.value = value;
        }

        @Override
        public void update() {
            setStatistics(this);
        }
    }

    /**
     * A stack distance histogram metric that fetches bucket counts from RocksDB on demand. It
     * implements both {@link StackDistanceHistogramProvider} (for the on-demand pull via {@link
     * MetricQueryService}) and {@link Gauge} (so it can be registered through the standard {@code
     * metricGroup.gauge()} path).
     *
     * <p>The bucket boundaries are fixed power-of-2 constants. The {@link
     * MetricQueryService#addMetric} method checks for {@code StackDistanceHistogramProvider} before
     * {@code Gauge}, so this metric is routed to the dedicated stack distance histogram map rather
     * than the gauges map.
     */
    class RocksDBStackDistanceHistogramView
            implements StackDistanceHistogramProvider, Gauge<String> {

        private final long[] bucketBoundaries = {
            1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024
        };

        @Override
        public long[] fetchBucketCounts() {
            synchronized (lock) {
                if (rocksDB == null) {
                    LOG.debug(
                            "RocksDB reference is null, returning empty stack distance histogram.");
                    return new long[bucketBoundaries.length + 1];
                }
                LOG.debug("Fetching stack distance histogram from RocksDB.");
                // TODO: Replace with actual RocksDB JNI call to fetch stack distance histogram
                // For now, return a zeroed array as a stub
                return new long[bucketBoundaries.length + 1];
            }
        }

        @Override
        public long[] getBucketBoundaries() {
            return bucketBoundaries;
        }

        @Override
        public String getValue() {
            long[] counts = fetchBucketCounts();
            StringBuilder sb = new StringBuilder();
            sb.append("{\"boundaries\":[");
            for (int i = 0; i < bucketBoundaries.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(bucketBoundaries[i]);
            }
            sb.append("],\"counts\":[");
            for (int i = 0; i < counts.length; i++) {
                if (i > 0) {
                    sb.append(',');
                }
                sb.append(counts[i]);
            }
            sb.append("]}");
            return sb.toString();
        }
    }
}
