package edu.berkeley.thebes.server.replica;

import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.server.AntiEntropyServer;
import edu.berkeley.thebes.server.persistence.IPersistenceEngine;

import java.nio.ByteBuffer;

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
    public boolean put(String key, ByteBuffer value) throws TException {
        antiEntropyServer.sendToNeighbors(key, value);
        return persistenceEngine.put(key, value);
    }

    @Override
    public ByteBuffer get(String key) {
        ByteBuffer ret = persistenceEngine.get(key);
        if (ret == null)
            ret = ByteBuffer.allocate(0);
        return ret;
    }
}