package edu.berkeley.thebes.server.replica;

import edu.berkeley.thebes.common.thrift.ThebesReplicaService;
import edu.berkeley.thebes.server.persistence.IPersistenceEngine;

import java.nio.ByteBuffer;

public class ThebesReplicaServiceHandler implements ThebesReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;

    public ThebesReplicaServiceHandler(IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    public boolean put(String key, ByteBuffer value) {
        return persistenceEngine.put(key, value);
    }

    public ByteBuffer get(String key) {
        return persistenceEngine.get(key);
    }
}