

package org.corfudb.universe.scenario;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.google.common.reflect.TypeToken;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.corfudb.protocols.wireprotocol.StreamAddressRange;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.StreamAddressSpace;
import org.corfudb.universe.GenericIntegrationTest;
import org.corfudb.universe.group.cluster.CorfuCluster;
import org.corfudb.universe.node.client.CorfuClient;
import org.junit.Test;
import org.rocksdb.CompressionType;
import org.rocksdb.Options;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class StreamBenchmark extends GenericIntegrationTest {

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

            final long WRITE_SIZE = 3000;
            final int DATA_SIZE_CHAR = 100;
            final int TX_SIZE = 10;
            final String tableName = "diskBackedMap";
            final UUID streamId = CorfuRuntime.getStreamID(tableName);

            Options options = new Options();
            options.setCreateIfMissing(true);
            options.setCompressionType(CompressionType.NO_COMPRESSION);

            CorfuTable<String, String> table1 = corfuClient.getRuntime().getObjectsView().build()
                    .setTypeToken(new TypeToken<CorfuTable<String, String>>() {})
                    .setStreamName(tableName)
                    .open();


            MetricRegistry metricRegistry = corfuClient.getRuntime().getDefaultMetrics();

            TriFunction<Integer, Integer, CorfuTable<String, String>, Void> unitOfWork =
                    (offset, delay, table) -> {
                        Timer writeTimer = metricRegistry.timer("write-duration-" + delay);
                        Timer putTimer = metricRegistry.timer("write-duration-put-" + delay);

                        for (long i = offset * WRITE_SIZE; i < WRITE_SIZE * offset + WRITE_SIZE; i++) {
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
                                if (i % 100 == 0) System.err.println(i + "; "  + watch.getTime());
                            } catch (TransactionAbortedException e) {
                                e.printStackTrace();
                            }
                        }
                        return null;
                    };


            Thread t0 = new Thread(() -> unitOfWork.apply(0, 0, table1));
            t0.start();

            try {
                t0.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }


            StreamAddressSpace space = corfuClient.getRuntime().getSequencerView()
                    .getStreamAddressSpace(new StreamAddressRange(streamId, Long.MAX_VALUE, 0));
            List<Long> addrs = Arrays.stream(space.getAddressMap().toArray()).boxed().collect(Collectors.toList());
            System.err.println("Size: " + addrs.size());

            CorfuClient newClient = corfuCluster.getLocalCorfuClient();
            StopWatch watch = new StopWatch();
            watch.start();
            for (Long addr: addrs) {
                newClient.getRuntime().getAddressSpaceView().read(addr);
            }
            watch.stop();
            System.err.println("Single : "  + watch.getTime());

            watch.reset();

            newClient = corfuCluster.getLocalCorfuClient();
            watch.start();
            newClient.getRuntime().getAddressSpaceView().read(addrs);
            watch.stop();
            System.err.println("Batch: "  + watch.getTime());
            corfuClient.shutdown();
        });
    }
}

