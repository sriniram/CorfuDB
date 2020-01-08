package org.corfudb.runtime.view;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import com.google.common.reflect.TypeToken;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.corfudb.protocols.logprotocol.LogEntry;
import org.corfudb.protocols.logprotocol.SMRLogEntry;
import org.corfudb.protocols.logprotocol.SMRRecord;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.TransactionAbortedException;
import org.corfudb.runtime.view.stream.IStreamView;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

/**
 * Created by mwei on 2/18/16.
 */
public class ObjectsViewTest extends AbstractViewTest {

    @Test
    @SuppressWarnings("unchecked")
    public void canAbortNoTransaction()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();
        r.getObjectsView().TXAbort();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void abortedTransactionDoesNotConflict()
            throws Exception {
        final String mapA = "map a";
        //Enable transaction logging
        CorfuRuntime r = getDefaultRuntime()
                .setTransactionLogging(true);

        SMRMap<String, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(mapA)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        // TODO: fix so this does not require mapCopy.
        SMRMap<String, String> mapCopy = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName(mapA)
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .option(ObjectOpenOption.NO_CACHE)
                .open();


        map.put("initial", "value");

        Semaphore s1 = new Semaphore(0);
        Semaphore s2 = new Semaphore(0);

        // Schedule two threads, the first starts a transaction and reads,
        // then waits for the second thread to finish.
        // the second starts a transaction, waits for the first tx to read
        // and commits.
        // The first thread then resumes and attempts to commit. It should abort.
        scheduleConcurrently(1, t -> {
            assertThatThrownBy(() -> {
                getRuntime().getObjectsView().TXBegin();
                map.get("k");
                s1.release();   // Let thread 2 start.
                s2.acquire();   // Wait for thread 2 to commit.
                map.put("k", "v1");
                getRuntime().getObjectsView().TXEnd();
            }).isInstanceOf(TransactionAbortedException.class);
        });

        scheduleConcurrently(1, t -> {
            s1.acquire();   // Wait for thread 1 to read
            getRuntime().getObjectsView().TXBegin();
            mapCopy.put("k", "v2");
            getRuntime().getObjectsView().TXEnd();
            s2.release();
        });

        executeScheduled(2, PARAMETERS.TIMEOUT_LONG);

        // The result should contain T2s modification.
        assertThat(map)
                .containsEntry("k", "v2");

        IStreamView txStream = r.getStreamsView().get(ObjectsView
                .TRANSACTION_STREAM_ID);
        List<ILogData> txns = txStream.remainingUpTo(Long.MAX_VALUE);
        assertThat(txns).hasSize(1);
        assertThat(txns.get(0).getLogEntry(getRuntime()).getType())
                .isEqualTo(LogEntry.LogEntryType.SMRLOG);

        SMRLogEntry tx1 = (SMRLogEntry)txns.get(0).getLogEntry
                (getRuntime());
        List<SMRRecord> streamUpdates = tx1.getEntryMap().get(CorfuRuntime.getStreamID(mapA));
        assertThat(streamUpdates).isNotNull();

        assertThat(streamUpdates.size()).isEqualTo(1);

        SMRRecord smrRecord = streamUpdates.get(0);
        Object[] args = smrRecord.getSMRArguments();
        assertThat(smrRecord.getSMRMethod()).isEqualTo("put");
        assertThat((String) args[0]).isEqualTo("k");
        assertThat((String) args[1]).isEqualTo("v2");
    }

    @Test
    public void incorrectNestingTest() {
        CorfuRuntime r1 = getDefaultRuntime();
        CorfuRuntime r2 = CorfuRuntime.fromParameters(CorfuRuntime.CorfuRuntimeParameters.builder()
                .nettyEventLoop(NETTY_EVENT_LOOP)
                .build());
        SMRMap<String, String> map = getDefaultRuntime().getObjectsView()
                .build()
                .setStreamName("mapa")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        r1.getObjectsView().TXBegin();
        map.get("key1");

        // Try to start a new nested transaction with a different runtime
        assertThatThrownBy(() -> r2.getObjectsView().TXBegin()).isInstanceOf(IllegalStateException.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unrelatedStreamDoesNotConflict()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> smrMap = r.getObjectsView().build()
                .setStreamName("map a")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        UUID streamBId = CorfuRuntime.getStreamID("b");
        IStreamView streamB = r.getStreamsView().get(streamBId);
        smrMap.put("a", "b");
        SMRLogEntry smrLogEntry = new SMRLogEntry();
        smrLogEntry.addTo(streamBId, new SMRRecord("hi", new Object[]{"hello"}, Serializers.PRIMITIVE));
        streamB.append(smrLogEntry);

        //this TX should not conflict
        assertThat(smrMap)
                .doesNotContainKey("b");
        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMap.put("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap)
                .containsEntry("b", "b");
    }

    @Test
    @SuppressWarnings("unchecked")
    public void unrelatedTransactionDoesNotConflict()
            throws Exception {
        //begin tests
        CorfuRuntime r = getDefaultRuntime();

        Map<String, String> smrMap = r.getObjectsView().build()
                .setStreamName("map a")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        Map<String, String> smrMapB = r.getObjectsView().build()
                .setStreamName("map b")
                .setTypeToken(new TypeToken<SMRMap<String, String>>() {})
                .open();

        smrMap.put("a", "b");

        r.getObjectsView().TXBegin();
        String b = smrMap.get("a");
        smrMapB.put("b", b);
        r.getObjectsView().TXEnd();

        //this TX should not conflict
        assertThat(smrMap)
                .doesNotContainKey("b");
        r.getObjectsView().TXBegin();
        b = smrMap.get("a");
        smrMap.put("b", b);
        r.getObjectsView().TXEnd();

        assertThat(smrMap)
                .containsEntry("b", "b");
    }

}
