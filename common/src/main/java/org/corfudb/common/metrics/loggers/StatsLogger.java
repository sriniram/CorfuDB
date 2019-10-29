package org.corfudb.common.metrics.loggers;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.google.common.annotations.VisibleForTesting;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Getter
public class StatsLogger {

    private final String loggerName;
    private final MetricRegistry registry;
    private final Map<String, StatsLogger> subLoggers;

    public StatsLogger(String loggerName) {
        this.loggerName = loggerName;
        registry = new MetricRegistry();
        subLoggers = new ConcurrentHashMap<>();
    }

    public StatsLogger getSubLogger(String subName) {
        subLoggers.putIfAbsent(subName, new StatsLogger(subName));
        return subLoggers.get(subName);
    }

    @VisibleForTesting
    public boolean containsMetric(String name) {
        return registry.getNames().contains(name);
    }
}
