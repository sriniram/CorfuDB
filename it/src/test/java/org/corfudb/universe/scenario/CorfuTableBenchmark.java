

package org.corfudb.universe.scenario;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.PersistedStreamingMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.group.cluster.CorfuClusterParams;
import org.corfudb.universe.node.client.CorfuClient;
import org.corfudb.universe.node.server.SupportServerParams;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

public class CorfuTableBenchmark extends GenericIntegrationTest {

    private static final long WRITE_SIZE = 1_000_000;
    @FunctionalInterface
    interface TriFunction<T, U, V, R> {
        R apply(T a, U b, V c);
    }

    @Test
    public void diskBackedTable() {

        final int metricsPort = 1234;
        workflow(wf -> {
            wf.setupDocker(fixture -> fixture.getSupportServer().metricPorts(Collections.singleton(metricsPort)));

            wf.deploy();
            CorfuCluster corfuCluster = (CorfuCluster) wf.getUniverse().groups().values().stream()
                    .filter(t -> t instanceof CorfuCluster).findFirst().get();
            CorfuClient corfuClient = corfuCluster.getLocalCorfuClient(metricsPort);

            final int DATA_SIZE_CHAR = 100;
            final int TX_SIZE = 1000;
            final Path persistedCacheLocation = Paths.get("/tmp/disk");

            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.NO_COMPRESSION);

            CorfuTable<String, String> table1 = corfuClient.getRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName("diskBackedMap")
                    .open();

            CorfuTable<String, String>  table2 = corfuClient.getRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName("memMap")
                    .open();

            MetricRegistry metricRegistry = corfuClient.getRuntime().getDefaultMetrics();


            TriFunction<Integer, Integer, CorfuTable<String, String>, Void> unitOfWork =
                    (offset, delay, table) -> {
                        Timer writeTimer = metricRegistry.timer("write-duration-" + delay);
                        Timer putTimer = metricRegistry.timer("write-duration-put-" + delay);

                        for (long i = offset * WRITE_SIZE; i < WRITE_SIZE * offset + WRITE_SIZE; i++) {
                            try {
                                Thread.sleep((long)(Math.random() * delay));
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }

                            try (Timer.Context context = writeTimer.time()) {
                                StopWatch watch = new StopWatch();
                                watch.start();
                                corfuClient.getRuntime().getObjectsView().TXBegin();
                                try (Timer.Context context2 = putTimer.time()) {
                                    for (int j = 0; j < TX_SIZE; j++) {
                                        table.put( i + "-" + j,
                                                RandomStringUtils.random(DATA_SIZE_CHAR, true, true));
                                    }
                                }
                                corfuClient.getRuntime().getObjectsView().TXEnd();
                                watch.stop();
                                System.err.println(i + "; "  + watch.getTime());
                            } catch (TransactionAbortedException e) {
                                e.printStackTrace();
                            }
                        }
                        return null;
                    };


            Thread t0 = new Thread(() -> unitOfWork.apply(0, 0, table1));
            t0.start();

            Thread t1 = new Thread(() -> unitOfWork.apply(1, 100, table1));
            t1.start();

            Thread t2 = new Thread(() -> unitOfWork.apply(2, 1000, table1));
            t2.start();

            Thread t3 = new Thread(() -> unitOfWork.apply(5, 1, table2));

            try {
                t0.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            corfuClient.shutdown();
        });
    }
}

