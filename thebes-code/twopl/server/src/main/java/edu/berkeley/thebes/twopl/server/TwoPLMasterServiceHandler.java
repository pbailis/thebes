package edu.berkeley.thebes.twopl.server;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;

public class TwoPLMasterServiceHandler implements TwoPLMasterReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private TwoPLLocalLockManager lockManager;
    private TwoPLSlaveReplicationService slaveReplicationService;

    public TwoPLMasterServiceHandler(IPersistenceEngine persistenceEngine,
            TwoPLLocalLockManager lockManager,
            TwoPLSlaveReplicationService slaveReplicationService) {
        this.persistenceEngine = persistenceEngine;
        this.lockManager = lockManager;
        this.slaveReplicationService = slaveReplicationService;
    }

    @Override
    public boolean lock(long sessionId, String key) throws TException {
        return lockManager.lock(key, sessionId);
    }

    @Override
    public boolean unlock(long sessionId, String key) throws TException {
        return lockManager.unlock(key, sessionId);
    }

    @Override
    public DataItem get(long sessionId, String key) throws TException {
        if (lockManager.ownsLock(key, sessionId)) {
            return persistenceEngine.get(key);
        } else {
            throw new TException("Session does not own GET lock on '" + key + "'");
        }
    }

    @Override
    public boolean put(long sessionId, String key, DataItem value) throws TException {
        if (lockManager.ownsLock(key, sessionId)) {
            boolean success = persistenceEngine.put(key, value);
            if (success) {
                slaveReplicationService.sendToSlaves(key, value);
            }
            return success;
        } else {
            throw new TException("Session does not own PUT lock on '" + key + "'");
        }
    }
}