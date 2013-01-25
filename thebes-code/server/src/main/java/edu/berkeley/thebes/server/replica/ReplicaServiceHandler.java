package edu.berkeley.thebes.server.replica;

import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.server.persistence.IPersistenceEngine;

import java.nio.ByteBuffer;

public class ReplicaServiceHandler implements ReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;

    public ReplicaServiceHandler(IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    public boolean put(String key, ByteBuffer value) {
        return persistenceEngine.put(key, value);
    }

    public ByteBuffer get(String key) {
        ByteBuffer ret = persistenceEngine.get(key);
        if (ret == null)
            ret = ByteBuffer.allocate(0);
        return ret;
    }
}