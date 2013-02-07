package edu.berkeley.thebes.twopl.server;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLSlaveReplicaService;

public class TwoPLSlaveServiceHandler implements TwoPLSlaveReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;

    public TwoPLSlaveServiceHandler(IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        System.out.println("PUT " + key + "!");
        return persistenceEngine.put(key, value);
    }
}