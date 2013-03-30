package edu.berkeley.thebes.hat.server.replica;

import com.google.common.collect.Maps;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;

import java.util.List;
import java.util.Map;

public class ReplicaServiceHandler implements ReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private AntiEntropyServiceRouter antiEntropyRouter;
    private DependencyResolver dependencyResolver;
    private static Logger logger = LoggerFactory.getLogger(ReplicaServiceHandler.class);


    public ReplicaServiceHandler(IPersistenceEngine persistenceEngine,
                                 AntiEntropyServiceRouter antiEntropyRouter,
                                 DependencyResolver dependencyResolver) {
        this.persistenceEngine = persistenceEngine;
        this.antiEntropyRouter = antiEntropyRouter;
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public boolean put(String key,
                       ThriftDataItem valueThrift) throws TException {
        DataItem value = new DataItem(valueThrift);
        if(logger.isTraceEnabled())
            logger.trace("received PUT request for key: '"+key+
                         "' value: '"+value+
                         "' transactionKeys: "+value.getTransactionKeys());

        antiEntropyRouter.sendWriteToSiblings(key, valueThrift);

        // TODO: Hmm, if siblings included us, we wouldn't even need to do this...
        if (value.getTransactionKeys() == null || value.getTransactionKeys().isEmpty()) {
            persistenceEngine.put(key, value);
        } else {
            dependencyResolver.addPendingWrite(key, value);
        }

        // todo: remove this return value--it's really not necessary
        return true;
    }

    @Override
    // TODO: can make substantially more efficient
    public boolean put_all(Map<String,ThriftDataItem> pairs) throws TException
    {
        for(String key : pairs.keySet()) {
            put(key, pairs.get(key));
        }

        return true;
    }

    @Override
    public ThriftDataItem get(String key, ThriftVersion requiredVersionThrift) throws TException {
        DataItem ret = persistenceEngine.get(key);
        Version requiredVersion = Version.fromThrift(requiredVersionThrift);
        
        if(logger.isTraceEnabled())
            logger.trace("received GET request for key: '"+key+
                         "' requiredVersion: "+ requiredVersion+
                         ", found version: " + (ret == null ? null : ret.getVersion()));

        if (requiredVersion != null && requiredVersion.compareTo(Version.NULL_VERSION) != 0 &&
                (ret == null || requiredVersion.compareTo(ret.getVersion()) > 0)) {
            ret = dependencyResolver.retrievePendingItem(key, requiredVersion);

            // race?
            if(ret == null) {
                logger.warn(String.format("Didn't find suitable version (timestamp=%d) for key %s in pending or persistenceEngine, so fetching again", requiredVersion.getTimestamp(), key));
                ret = persistenceEngine.get(key);
            }

            if(ret == null || requiredVersion.compareTo(ret.getVersion()) > 0) {
                logger.error(String.format("suitable version was not found! required time: %d clientID: %d only got %s",
                                                   requiredVersion.getTimestamp(), requiredVersion.getClientID(),
                                                   ret == null ? "null" : Long.toString(ret.getVersion().getTimestamp())));
                ret = null;
            }
        }

        if(ret == null) {
            return new ThriftDataItem().setVersion(Version.toThrift(Version.NULL_VERSION));
        }

        return ret.toThrift();
    }

    @Override
    // TODO: can make substantially more efficient
    public Map<String, ThriftDataItem> get_all(Map<String, ThriftVersion> keys) throws TException {
        Map<String, ThriftDataItem> ret = Maps.newHashMap();

        for(String key : keys.keySet()) {
            ret.put(key, get(key, keys.get(key)));
        }

        return ret;
    }
}