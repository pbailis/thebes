package edu.berkeley.thebes.hat.server.replica;

import edu.berkeley.thebes.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.server.persistence.IPersistenceEngine;

import java.nio.ByteBuffer;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private IPersistenceEngine persistenceEngine;

    public AntiEntropyServiceHandler(IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    public boolean put(String key, ByteBuffer value) {
        return persistenceEngine.put(key, value);
    }
}