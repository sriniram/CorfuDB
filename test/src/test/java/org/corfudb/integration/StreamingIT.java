package org.corfudb.integration;


import lombok.Getter;

import org.corfudb.runtime.CorfuStoreMetadata.Timestamp;
import org.corfudb.runtime.collections.CorfuStore;
import org.corfudb.runtime.collections.CorfuStreamEntries;
import org.corfudb.runtime.collections.CorfuStreamEntry;
import org.corfudb.runtime.collections.StreamListener;
import org.corfudb.runtime.collections.Table;
import org.corfudb.runtime.collections.TableSchema;
import org.corfudb.runtime.collections.TableOptions;
import org.corfudb.runtime.collections.TxBuilder;
import org.corfudb.test.SampleSchema.Uuid;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Simple test that inserts data into CorfuStore and tests Streaming.
 */
public class StreamingIT extends AbstractIT {

    private static String corfuSingleNodeHost;
    private static int corfuStringNodePort;
    private static String singleNodeEndpoint;

    /* A helper method that takes host and port specification, start a single server and
     *  returns a process. */
    private Process runSinglePersistentServer(String host, int port) throws IOException {
        return new AbstractIT.CorfuServerRunner()
                .setHost(host)
                .setPort(port)
                .setLogPath(getCorfuServerLogPath(host, port))
                .setSingle(true)
                .runServer();
    }

    /**
     * Load properties for a single node corfu server before each test
     */
    @Before
    public void loadProperties() {
        corfuSingleNodeHost = PROPERTIES.getProperty("corfuSingleNodeHost");
        corfuStringNodePort = Integer.valueOf(PROPERTIES.getProperty("corfuSingleNodePort"));
        singleNodeEndpoint = String.format("%s:%d",
                corfuSingleNodeHost,
                corfuStringNodePort);
    }

    /**
     * A StreamListener implementation to be used in the tests.
     *
     * This listener accumulates all updates streamed to it into a linked list that
     * can then be used to verify.
     */
    private class StreamListenerImpl implements StreamListener {

        @Getter
        private final String name;

        @Getter
        private final LinkedList<CorfuStreamEntries> updates = new LinkedList<>();
        
        public StreamListenerImpl(String name) {
            this.name = name;
        }

        @Override
        public void onNext(CorfuStreamEntries results) {
            updates.add(results);
        }

        @Override
        public void onError(Throwable throwable) {
        }

        @Override
        public boolean equals(StreamListener o) {
            return this.hashCode() == o.hashCode();
        }

        @Override
        public int hashcode() {
            return name.hashCode();
        }

        @Override
        public String toString() {
            return name;
        }
    }

    /**
     * Basic Streaming Test with a single table.
     *
     * The test updates a single table a few times and ensures that the listeners subscribed
     * to the streaming updates from the table get the updates.
     *
     * The test verifies the following:
     * 1. Streaming subscriptions with different timestamps get the right set of updates.
     * 2. The operation type, key, payload and metadata values are as expected.
     * 3. A listener does not get stream updates after it has been unsubscribed.
     */
    @Test
    public void testStreamingSingleTable() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        runtime.setTransactionLogging(true);
        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = store.getTimestamp();

        // Create a table
        Table<Uuid, Uuid, Uuid> n1t1 = store.openTable(
                "n1", "t1", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.builder().build()
        );

        // Make some updates to the table.
        final int numUpdates = 3;
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            TxBuilder tx = store.tx("n1");
            tx.update("t1", uuid, uuid, uuid).commit();
        }

        // Subscribe to streaming updates from the table.
        StreamListenerImpl s1n1t1 = new StreamListenerImpl("s1n1t1");
        store.subscribe(s1n1t1, "n1",
                Collections.singletonList(new TableSchema("t1", Uuid.class, Uuid.class, Uuid.class)), ts1);

        // After a brief wait verify that the listener gets all the updates.
        TimeUnit.SECONDS.sleep(2);

        LinkedList<CorfuStreamEntries> updates = s1n1t1.getUpdates();
        assertThat(updates.size() == numUpdates).isTrue();
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            CorfuStreamEntries update = updates.get(i);
            assertThat(update.getEntries().size() == 1).isTrue();
            List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
            assertThat(entry.size() == 1).isTrue();
            assertThat(entry.get(0).getOperation().equals(CorfuStreamEntry.OperationType.UPDATE)).isTrue();
            assertThat(entry.get(0).getKey().equals(uuid)).isTrue();
            assertThat(entry.get(0).getPayload().equals(uuid)).isTrue();
            assertThat(entry.get(0).getMetadata().equals(uuid)).isTrue();
        }

        // Add another subscriber to the same table starting now.
        StreamListenerImpl s2n1t1 = new StreamListenerImpl("s2n1t1");
        store.subscribe(s2n1t1, "n1",
                Collections.singletonList(new TableSchema("t1", Uuid.class, Uuid.class, Uuid.class)), null);

        TxBuilder tx = store.tx("n1");
        Uuid uuid0 = Uuid.newBuilder().setMsb(0).setLsb(0).build();
        tx.delete("t1", uuid0).commit();

        TimeUnit.SECONDS.sleep(2);

        // Both the listener should see the deletion.
        updates = s1n1t1.getUpdates();
        CorfuStreamEntries update = updates.getLast();
        assertThat(update.getEntries().size() == 1).isTrue();
        List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry.size() == 1).isTrue();
        assertThat(entry.get(0).getOperation().equals(CorfuStreamEntry.OperationType.DELETE)).isTrue();
        assertThat(entry.get(0).getKey().equals(uuid0)).isTrue();
        assertThat(entry.get(0).getPayload() == null).isTrue();
        assertThat(entry.get(0).getMetadata() == null).isTrue();

        updates = s2n1t1.getUpdates();
        // Ensure that the s2n1t1 listener sees only one update, i.e the last delete.
        assertThat(updates.size() == 1).isTrue();
        update = updates.getLast();
        assertThat(update.getEntries().size() == 1).isTrue();
        entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry.size() == 1).isTrue();
        assertThat(entry.get(0).getOperation().equals(CorfuStreamEntry.OperationType.DELETE)).isTrue();
        assertThat(entry.get(0).getKey().equals(uuid0)).isTrue();
        assertThat(entry.get(0).getPayload() == null).isTrue();
        assertThat(entry.get(0).getMetadata() == null).isTrue();

        // Unsubscribe s1n1t1 and ensure that it no longer gets any updates.
        store.unsubscribe(s1n1t1);
        TimeUnit.SECONDS.sleep(2);

        tx = store.tx("n1");
        Uuid uuid1 = Uuid.newBuilder().setMsb(1).setLsb(1).build();
        tx.delete("t1", uuid1).commit();

        // s1n1t1 should see no new updates, where as s2n1t1 should see the latest delete.
        TimeUnit.SECONDS.sleep(2);
        updates = s1n1t1.getUpdates();
        assertThat(updates.size() == numUpdates + 1).isTrue();
        updates = s2n1t1.getUpdates();
        assertThat(updates.size() == 2).isTrue();
        update = updates.getLast();
        assertThat(update.getEntries().size() == 1).isTrue();
        entry = update.getEntries().values().stream().findFirst().get();
        assertThat(entry.size() == 1).isTrue();
        assertThat(entry.get(0).getOperation().equals(CorfuStreamEntry.OperationType.DELETE)).isTrue();
        assertThat(entry.get(0).getKey().equals(uuid1)).isTrue();


        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

    /**
     * Streaming Test with two different tables and two separate streamers.
     * <p>
     * The test updates two tables a few times and ensures that the listeners subscribed
     * to the streaming updates from the table get the updates.
     * <p>
     * The test verifies the following:
     * 1. Streaming subscriptions with different timestamps get the right set of updates.
     * 2. The operation type, key, payload and metadata values are as expected.
     */
    @Test
    public void testStreamingMultiTableStreams() throws Exception {
        // Run a corfu server
        Process corfuServer = runSinglePersistentServer(corfuSingleNodeHost, corfuStringNodePort);

        // Start a Corfu runtime
        runtime = createRuntime(singleNodeEndpoint);

        runtime.setTransactionLogging(true);
        CorfuStore store = new CorfuStore(runtime);

        // Record the initial timestamp.
        Timestamp ts1 = store.getTimestamp();

        // Create 2 tables
        Table<Uuid, Uuid, Uuid> n1t1 = store.openTable(
                "n1", "t1", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.builder().build()
        );

        Table<Uuid, Uuid, Uuid> n2t1 = store.openTable(
                "n2", "t1", Uuid.class,
                Uuid.class, Uuid.class,
                TableOptions.builder().build()
        );

        // Make some updates to the table.
        final int numUpdates = 3;
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            TxBuilder tx = store.tx("n1");
            tx.update("t1", uuid, uuid, uuid).commit();
            TxBuilder tx2 = store.tx("n2");
            tx2.update("t1", uuid, uuid, uuid).commit();
        }

        // Subscribe to streaming updates from the table1.
        StreamListenerImpl s1n1t1 = new StreamListenerImpl("s1n1t1");
        // Subscribe to streaming updates from the table2.
        StreamListenerImpl s2n2t1 = new StreamListenerImpl("s2n2t1");
        store.subscribe(s1n1t1, "n1",
                Collections.singletonList(new TableSchema("t1", Uuid.class, Uuid.class, Uuid.class)), ts1);
        store.subscribe(s2n2t1, "n2",
                Collections.singletonList(new TableSchema("t1", Uuid.class, Uuid.class, Uuid.class)), ts1);

        // After a brief wait verify that the listener go all the updates.
        TimeUnit.SECONDS.sleep(2);
        LinkedList<CorfuStreamEntries> updates = s1n1t1.getUpdates();
        assertThat(updates.size()).isEqualTo(numUpdates);
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            CorfuStreamEntries update = updates.get(i);
            assertThat(update.getEntries()).hasSize(1);
            List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
            assertThat(entry).hasSize(1);
            assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
            assertThat(entry.get(0).getKey()).isEqualTo(uuid);
            assertThat(entry.get(0).getPayload()).isEqualTo(uuid);
            assertThat(entry.get(0).getMetadata()).isEqualTo(uuid);
        }

        LinkedList<CorfuStreamEntries> updates2 = s2n2t1.getUpdates();
        assertThat(updates2.size()).isEqualTo(numUpdates);
        for (int i = 0; i < numUpdates; i++) {
            Uuid uuid = Uuid.newBuilder().setMsb(i).setLsb(i).build();
            CorfuStreamEntries update = updates2.get(i);
            assertThat(update.getEntries()).hasSize(1);
            List<CorfuStreamEntry> entry = update.getEntries().values().stream().findFirst().get();
            assertThat(entry).hasSize(1);
            assertThat(entry.get(0).getOperation()).isEqualTo(CorfuStreamEntry.OperationType.UPDATE);
            assertThat(entry.get(0).getKey()).isEqualTo(uuid);
            assertThat(entry.get(0).getPayload()).isEqualTo(uuid);
            assertThat(entry.get(0).getMetadata()).isEqualTo(uuid);
        }

        assertThat(shutdownCorfuServer(corfuServer)).isTrue();
    }

}
