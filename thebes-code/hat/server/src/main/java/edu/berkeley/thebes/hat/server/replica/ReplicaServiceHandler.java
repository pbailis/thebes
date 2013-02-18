package edu.berkeley.thebes.hat.server.replica;

import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import edu.berkeley.thebes.hat.server.dependencies.GenericDependencyChecker;
import edu.berkeley.thebes.hat.server.dependencies.GenericDependencyResolver;
import org.apache.thrift.TException;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServer;

import java.util.List;

public class ReplicaServiceHandler implements ReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private AntiEntropyServer antiEntropyServer;
    private GenericDependencyResolver causalDependencyResolver;

    public ReplicaServiceHandler(IPersistenceEngine persistenceEngine,
                                 AntiEntropyServer antiEntropyServer,
                                 GenericDependencyResolver causalDependencyResolver) {
        this.persistenceEngine = persistenceEngine;
        this.antiEntropyServer = antiEntropyServer;
        this.causalDependencyResolver = causalDependencyResolver;
    }

    @Override
    public boolean put(String key, DataItem value, List<DataDependency> happensAfter) throws TException {
        antiEntropyServer.sendToNeighbors(key, value, happensAfter);
        boolean ret = persistenceEngine.put(key, value);
        causalDependencyResolver.notifyNewLocalWrite(key);
        return ret;
    }

    @Override
    public DataItem get(String key) {
        DataItem ret = persistenceEngine.get(key);
        return ret;
    }
}