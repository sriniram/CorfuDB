package org.corfudb.runtime.view;

import org.apache.zookeeper.KeeperException;
import org.cliffc.high_scale_lib.NonBlockingHashMap;
import org.corfudb.runtime.CorfuDBRuntime;
import org.corfudb.runtime.collections.CDBSimpleMap;
import org.corfudb.runtime.smr.ICorfuDBObject;
import org.corfudb.runtime.smr.legacy.CorfuDBObject;
import org.corfudb.runtime.stream.IStream;
import org.corfudb.runtime.stream.IStreamMetadata;
import org.corfudb.runtime.stream.SimpleStream;
import org.corfudb.runtime.stream.SimpleStreamMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Created by mwei on 5/22/15.
 */
public class LocalCorfuDBInstance implements ICorfuDBInstance {

    private static final Logger log = LoggerFactory.getLogger(LocalCorfuDBInstance.class);

    // Members of this CorfuDBInstance
    private IConfigurationMaster configMaster;
    private IStreamingSequencer streamingSequencer;
    private IWriteOnceAddressSpace addressSpace;
    private CorfuDBRuntime cdr;
    private CDBSimpleMap<UUID, IStreamMetadata> streamMap;
    private ConcurrentMap<UUID, ICorfuDBObject> objectMap;

    // Classes to instantiate.
    private Class<? extends IStream> streamType;


    public LocalCorfuDBInstance(CorfuDBRuntime cdr)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        this(cdr, ConfigurationMaster.class, StreamingSequencer.class, WriteOnceAddressSpace.class,
                SimpleStream.class);
    }

    public LocalCorfuDBInstance(CorfuDBRuntime cdr,
                                Class<? extends IConfigurationMaster> cm,
                                Class<? extends IStreamingSequencer> ss,
                                Class<? extends IWriteOnceAddressSpace> as,
                                Class<? extends IStream> streamType)
            throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException
    {
        configMaster = cm.getConstructor(CorfuDBRuntime.class).newInstance(cdr);
        streamingSequencer = ss.getConstructor(CorfuDBRuntime.class).newInstance(cdr);
        addressSpace = as.getConstructor(CorfuDBRuntime.class).newInstance(cdr);

        this.streamType = streamType;
        this.objectMap = new NonBlockingHashMap<UUID, ICorfuDBObject>();
        this.cdr = cdr;
    }

    /**
     * Gets a configuration master for this instance.
     *
     * @return The configuration master for this instance.
     */
    @Override
    public IConfigurationMaster getConfigurationMaster() {
        return configMaster;
    }

    /**
     * Gets a streaming sequencer for this instance.
     *
     * @return The streaming sequencer for this instance.
     */
    @Override
    public IStreamingSequencer getStreamingSequencer() {
        return streamingSequencer;
    }

    /**
     * Gets a sequencer (regular) for this instance.
     *
     * @return The sequencer for this instance.
     */
    @Override
    public ISequencer getSequencer() {
        return streamingSequencer;
    }

    /**
     * Gets a write-once address space for this instance.
     *
     * @return A write-once address space for this instance.
     */
    @Override
    public IWriteOnceAddressSpace getAddressSpace() {
        return addressSpace;
    }

    /**
     * Gets a unique identifier for this instance.
     *
     * @return A unique identifier for this instance.
     */
    @Override
    public UUID getUUID() {
        return cdr.getView().getUUID();
    }

    /**
     * Gets the current view of this instance.
     *
     * @return A view of this instance.
     */
    @Override
    public CorfuDBView getView() {
        return cdr.getView();
    }

    /**
     * Opens a stream given it's identifier using this instance, or creates it
     * on this instance if one does not exist.
     *
     * @param id The unique ID of the stream to be opened.
     * @return The stream, if it exists. Otherwise, a new stream is created
     * using this instance.
     */
    @Override
    public IStream openStream(UUID id) {
        try {
            return streamType.getConstructor(UUID.class, CorfuDBRuntime.class)
                    .newInstance(id, cdr);
        }
        catch (InstantiationException | NoSuchMethodException | IllegalAccessException
                | InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }

    /**
     * Delete a stream given its identifier using this instance.
     *
     * @param id The unique ID of the stream to be deleted.
     * @return True, if the stream was successfully deleted, or false if there
     * was an error deleting the stream (does not exist).
     */
    @Override
    public boolean deleteStream(UUID id)
    {
        throw new UnsupportedOperationException("Currently unsupported!");
    }

    /**
     * Retrieves the stream metadata map for this instance.
     *
     * @return The stream metadata map for this instance.
     */
    @Override
    public Map<UUID, IStreamMetadata> getStreamMetadataMap() {
        /* for now, the stream metadata is backed on a CDBSimpleMap
            This could change if we need to support hopping, since
            there needs to be a globally consistent view of the stream
            start positions.
         */
        return streamMap;
    }

    /**
     * Retrieves a corfuDB object.
     *
     * @param id   A unique ID for the object to be retrieved.
     * @param type The type of object to instantiate.
     * @param args A list of arguments to pass to the constructor.
     * @return A CorfuDB object. A cached object may be returned
     * if one already exists in the system. A new object
     * will be created if one does not already exist.
     */
    @Override
    @SuppressWarnings("unchecked")
    public <T extends ICorfuDBObject> T openObject(UUID id, Class<T> type, Class<?>... args) {
        T cachedObject = (T) objectMap.getOrDefault(id, null);

        if (cachedObject != null)
        {
            if (!(type.isInstance(cachedObject)))
                throw new RuntimeException("Incorrect type! Requested to open object of type " + type.getClass() +
                        " but an object of type " + cachedObject.getClass() + " is already there!");
            return cachedObject;
        }


        try {
            List<Class<?>> classes = Arrays.stream(args)
                    .map(Class::getClass)
                    .collect(Collectors.toList());

            classes.add(0, IStream.class);

            List<Object> largs = Arrays.stream(args)
                    .collect(Collectors.toList());
            largs.add(openStream(id));

            T returnObject = type
                    .getConstructor(classes.toArray(new Class[classes.size()]))
                    .newInstance(largs.toArray(new Object[largs.size()]));

            objectMap.put(id, returnObject);
            return returnObject;
        }
        catch (NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e )
        {
            throw new RuntimeException(e);
        }
    }
}
