package edu.berkeley.thebes.hat.server.replica;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private IPersistenceEngine persistenceEngine;

    public AntiEntropyServiceHandler(IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    public boolean put(String key, DataItem value) {
        return persistenceEngine.put(key, value);
    }
}