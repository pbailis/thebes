package edu.berkeley.thebes.hat.server.replica;

import java.util.List;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ThriftDataDependency;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;
import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServer;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;
import edu.berkeley.thebes.hat.server.dependencies.PendingWrites;

public class ReplicaServiceHandler implements ReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private AntiEntropyServer antiEntropyServer;
    private DependencyResolver dependencyResolver;
    private PendingWrites pendingWrites;

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
                       ThriftDataItem value,
                       List<ThriftDataDependency> happensAfter,
                       List<String> transactionKeys) throws TException {
        antiEntropyServer.sendToNeighbors(key, value, happensAfter, transactionKeys);

        dependencyResolver.asyncApplyNewLocalWrite(key,
        		DataItem.fromThrift(value),
        		transactionKeys);

        // todo: remove this return value--it's really not necessary
        return true;
    }

    @Override
    public ThriftDataItem get(String key, ThriftVersion requiredVersion) throws TException {
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