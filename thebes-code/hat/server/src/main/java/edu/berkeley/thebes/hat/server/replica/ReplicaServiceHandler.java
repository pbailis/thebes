package edu.berkeley.thebes.hat.server.replica;

import java.util.List;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;

import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServer;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;
import edu.berkeley.thebes.hat.server.dependencies.PendingWrites;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplicaServiceHandler implements ReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private AntiEntropyServer antiEntropyServer;
    private DependencyResolver dependencyResolver;
    private PendingWrites pendingWrites;
    private static Logger logger = LoggerFactory.getLogger(ReplicaServiceHandler.class);


    public ReplicaServiceHandler(IPersistenceEngine persistenceEngine,
                                 PendingWrites pendingWrites,
                                 AntiEntropyServer antiEntropyServer,
                                 DependencyResolver dependencyResolver) {
        this.persistenceEngine = persistenceEngine;
        this.pendingWrites = pendingWrites;
        this.antiEntropyServer = antiEntropyServer;
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public boolean put(String key,
                       ThriftDataItem value) throws TException {
        if(logger.isTraceEnabled())
            logger.trace("received PUT request for key: '"+key+
                         "' value: '"+value+
                         "' transactionKeys: "+value.getTransactionKeys());

        antiEntropyServer.sendToNeighbors(key, value);

        dependencyResolver.asyncApplyNewWrite(key, DataItem.fromThrift(value));

        // todo: remove this return value--it's really not necessary
        return true;
    }

    @Override
    public ThriftDataItem get(String key, ThriftVersion requiredVersion) throws TException {
        if(logger.isTraceEnabled())
            logger.trace("received GET request for key: '"+key+
                         "' requiredVersion: "+requiredVersion);

        DataItem ret = persistenceEngine.get(key);

        if (requiredVersion != null &&
           (ret == null || ret.getVersion().compareTo(Version.fromThrift(requiredVersion)) <= 0)) {
            ret = pendingWrites.getMatchingItem(key, Version.fromThrift(requiredVersion));

            if (ret == null)
                throw new TException(String.format("suitable version was not found! time: %d clientID: %d",
                                                   requiredVersion.getTimestamp(), requiredVersion.getClientID()));
        }

        return DataItem.toThrift(ret);
    }
}