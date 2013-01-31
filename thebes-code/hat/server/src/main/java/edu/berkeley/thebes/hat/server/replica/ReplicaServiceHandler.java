package edu.berkeley.thebes.hat.server.replica;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.AntiEntropyServer;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;

import org.apache.thrift.TException;

public class ReplicaServiceHandler implements ReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private AntiEntropyServer antiEntropyServer;

    public ReplicaServiceHandler(IPersistenceEngine persistenceEngine,
            AntiEntropyServer antiEntropyServer) {
        this.persistenceEngine = persistenceEngine;
        this.antiEntropyServer = antiEntropyServer;
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        antiEntropyServer.sendToNeighbors(key, value);
        return persistenceEngine.put(key, value);
    }

    @Override
    public DataItem get(String key) {
        DataItem ret = persistenceEngine.get(key);
        return ret;
    }
}