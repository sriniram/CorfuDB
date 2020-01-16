package org.corfudb.infrastructure;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.MultipleReadRequest;
import org.corfudb.protocols.wireprotocol.MultipleWriteMsg;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.util.serializer.Serializers;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerAssertions.assertThat;
import static org.corfudb.infrastructure.LogUnitServerTest.waitForLogUnit;

/**
 * Created by WenbinZhu on 5/30/19.
 */
public class LogUnitCacheTest extends AbstractServerTest {

    private static final double MIN_HEAP_RATIO = 0.1;
    private static final double MAX_HEAP_RATIO = 0.9;

    @Override
    public AbstractServer getDefaultServer() {
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;

        return new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .build());
    }

    /**
     * Test non-cacheable reads on log unit sever will not affect server cache.
     */
    @Test
    public void checkNonCacheableReads() throws Exception {
        final int size = 10;
        final long start = 0L;
        final long end = start + size;

        LogUnitServer logUnitServer = (LogUnitServer) getDefaultServer();
        setServer(logUnitServer);

        List<Long> addresses = LongStream.range(start, end).boxed().collect(Collectors.toList());
        List<LogData> payloads = new ArrayList<>();

        for (long i = start; i < end; i++) {
            ByteBuf payload = Unpooled.buffer();
            Serializers.CORFU.serialize("hello".getBytes(), payload);
            LogData logData = new LogData(DataType.DATA, payload);
            logData.setGlobalAddress(i);
            payloads.add(logData);
        }

        // Range write is not cached on server.
        sendMessage(CorfuMsgType.MULTIPLE_WRITE.payloadMsg(new MultipleWriteMsg(payloads)));
        waitForLogUnit(logUnitServer);

        // Non-cacheable reads should not affect the data cache on server.
        sendMessage(CorfuMsgType.MULTIPLE_READ_REQUEST.payloadMsg(new MultipleReadRequest(addresses, false)));
        waitForLogUnit(logUnitServer);

        checkReadResponse(getLastPayloadMessageAs(ReadResponse.class), size);
        assertThat(logUnitServer.getDataCache().getSize()).isEqualTo(0);

        // Cacheable reads should update the data cache on server.
        sendMessage(CorfuMsgType.MULTIPLE_READ_REQUEST.payloadMsg(new MultipleReadRequest(addresses, true)));
        waitForLogUnit(logUnitServer);

        checkReadResponse(getLastPayloadMessageAs(ReadResponse.class), size);
        assertThat(logUnitServer.getDataCache().getSize()).isEqualTo(size);
    }

    private void checkReadResponse(ReadResponse readResponse, int size) {
        assertThat(readResponse.getAddresses().size()).isEqualTo(size);

        readResponse.getAddresses().forEach((addr, ld) -> assertThat(ld.getType()).isEqualTo(DataType.DATA));
    }

    /**
     * Test maximum server cache size is correctly set.
     */
    @Test
    public void CheckMaxCacheSizeIsCorrectRatio() {
        Random r = new Random(System.currentTimeMillis());
        double randomCacheRatio = MIN_HEAP_RATIO + (MAX_HEAP_RATIO - MIN_HEAP_RATIO) * r.nextDouble();
        String serviceDir = PARAMETERS.TEST_TEMP_DIR;
        LogUnitServer s1 = new LogUnitServer(new ServerContextBuilder()
                .setLogPath(serviceDir)
                .setMemory(false)
                .setCacheSizeHeapRatio(String.valueOf(randomCacheRatio))
                .build());

        assertThat(s1).hasMaxCorrectCacheSize(randomCacheRatio);
    }
}
