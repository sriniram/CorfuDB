package org.corfudb.common.metrics;

import com.codahale.metrics.MetricRegistry;
import lombok.extern.slf4j.Slf4j;
import org.corfudb.common.metrics.loggers.StatsLogger;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * ReportUtils is a temporary stream stats console log reporter.
 */
@Slf4j
public class ReportUtils {
    public static void reportStreamsThroughput(StatsLogger streamStatsLogger, int interval) {
        Map<String, StatsLogger> streams = streamStatsLogger.getSubLoggers();
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> {
            StringBuilder builder = new StringBuilder();
            for (Map.Entry<String, StatsLogger> entry : streams.entrySet()) {
                String streamID = entry.getKey();
                StatsLogger stats = entry.getValue();
                MetricRegistry registry = stats.getRegistry();

                double access = registry.meter("access-meter").getOneMinuteRate();
                double update = registry.meter("update-meter").getOneMinuteRate();
                double updateSize = registry.histogram("update-histogram").getSnapshot().getMean();

                builder.append(String.format("stream uuid: %s, access rate: %.3f op/s, " +
                                "logUpdate rate: %.3f op/s, logUpdate avg size is %.3f bytes\n",
                        streamID, access, update, updateSize));
            }

            if (builder.length() != 0) {
                log.info(builder.toString());
            }

        }, 0, interval, TimeUnit.SECONDS);
    }
}
