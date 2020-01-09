package org.corfudb.infrastructure.log;

import lombok.Builder;
import lombok.Builder.Default;
import lombok.ToString;
import org.corfudb.format.Types;
import org.corfudb.infrastructure.log.CompactionPolicy.CompactionPolicyType;
import org.corfudb.infrastructure.log.compression.Codec;

import java.util.concurrent.TimeUnit;


/**
 * This class specifies parameters for stream log implementation
 * and the stream log compactor.
 *
 * Created by WenbinZhu on 5/22/19.
 */
@Builder
@ToString
public class StreamLogParams {

    // Region: static members
    public static final int VERSION = 2;

    public static final int METADATA_SIZE = Types.Metadata.newBuilder()
            .setLengthChecksum(-1)
            .setPayloadChecksum(-1)
            .setLength(-1)
            .build()
            .getSerializedSize();

    public static final int RECORDS_PER_SEGMENT = 20_000;

    // End region

    // Region: stream log parameters
    public String logPath;

    @Default
    public boolean verifyChecksum;

    @Default
    public double logSizeQuotaPercentage;

    @Default
    public Codec.Type compressionCodec;
    // End region

    // Region: compactor parameters
    @Default
    public CompactionPolicyType compactionPolicyType;

    @Default
    public int compactionInitialDelayMin;

    @Default
    public int compactionPeriodMin;

    @Default
    public int compactionWorkers;

    @Default
    public int maxSegmentsForCompaction;

    @Default
    public int protectedSegments;

    @Default
    public double segmentGarbageRatioThreshold;

    @Default
    public double segmentGarbageSizeThresholdMB;

    @Default
    public double totalGarbageSizeThresholdMB;
    // End region
}
