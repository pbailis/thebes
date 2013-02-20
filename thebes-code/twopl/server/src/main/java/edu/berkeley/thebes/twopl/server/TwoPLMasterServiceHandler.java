package edu.berkeley.thebes.twopl.server;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.twopl.server.TwoPLLocalLockManager.LockType;

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
    public void write_lock(long sessionId, String key) throws TException {
        lockManager.lock(LockType.WRITE, key, sessionId);
    }
    
    @Override
    public void read_lock(long sessionId, String key) throws TException {
        lockManager.lock(LockType.READ, key, sessionId);
    }

    @Override
    public void unlock(long sessionId, String key) throws TException {
        lockManager.unlock(key, sessionId);
    }

    @Override
    public ThriftDataItem get(long sessionId, String key) throws TException {
        if (lockManager.ownsLock(LockType.READ, key, sessionId)) {
            return DataItem.toThrift(persistenceEngine.get(key));
        } else {
            throw new TException("Session does not own GET lock on '" + key + "'");
        }
    }

    @Override
    public boolean put(long sessionId, String key, ThriftDataItem value) throws TException {
        if (lockManager.ownsLock(LockType.WRITE, key, sessionId)) {
            boolean success = persistenceEngine.put(key, DataItem.fromThrift(value));
            if (success) {
                slaveReplicationService.sendToSlaves(key, value);
            }
            return success;
        } else {
            throw new TException("Session does not own PUT lock on '" + key + "'");
        }
    }
}