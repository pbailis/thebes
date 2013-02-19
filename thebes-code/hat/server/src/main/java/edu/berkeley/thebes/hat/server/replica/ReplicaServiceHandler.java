package edu.berkeley.thebes.hat.server.replica;

import edu.berkeley.thebes.common.thrift.ThriftUtil;
import edu.berkeley.thebes.common.thrift.ThriftUtil.VersionCompare;
import edu.berkeley.thebes.common.thrift.Version;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;
import edu.berkeley.thebes.hat.server.dependencies.PendingWrites;
import org.apache.thrift.TException;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServer;

import java.util.List;

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
                       DataItem value,
                       List<DataDependency> happensAfter,
                       List<String> transactionKeys) throws TException {
        antiEntropyServer.sendToNeighbors(key, value, happensAfter, transactionKeys);

        dependencyResolver.asyncApplyNewLocalWrite(key,
                                                   value,
                                                   transactionKeys);

        // todo: remove this return value--it's really not necessary
        return true;
    }

    @Override
    public DataItem get(String key,
                        Version requiredVersion) throws TException {
        DataItem ret = persistenceEngine.get(key);

        if(requiredVersion != null &&
           (ret == null || ThriftUtil.compareVersions(ret.getVersion(), requiredVersion) != VersionCompare.LATER)) {
            ret = pendingWrites.getMatchingItem(key, requiredVersion);

            if(ret == null)
                throw new TException(String.format("suitable version was not found! time: %d clientID: %d",
                                                   requiredVersion.getTimestamp(), requiredVersion.getClientID()));
        }

        return ret;
    }
}