package edu.berkeley.thebes.twopl.server;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.AntiEntropyServer;

import org.apache.thrift.TException;

public class TwoPLMasterServiceHandler implements TwoPLMasterReplicaService.Iface {
    private IPersistenceEngine persistenceEngine;
    private TwoPLLocalLockManager lockManager;

    public TwoPLMasterServiceHandler(IPersistenceEngine persistenceEngine,
            TwoPLLocalLockManager lockManager) {
        this.persistenceEngine = persistenceEngine;
        this.lockManager = lockManager;
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
            return persistenceEngine.put(key, value);
        } else {
            throw new TException("Session does not own PUT lock on '" + key + "'");
        }
    }
}