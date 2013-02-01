package edu.berkeley.thebes.twopl.server;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class TwoPLLocalLockManager {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(TwoPLLocalLockManager.class);
    
    /** A lock held by a particular session. */
    private static class TwoPLLock {
        private long sessionId;
        private boolean held;
        
        private TwoPLLock(long sessionId) {
            this.sessionId = sessionId;
        }
        
        public static TwoPLLock createAndAcquire(long sessionId) { 
            TwoPLLock lock = new TwoPLLock(sessionId);
            lock.held = true;
            return lock;
        }
        
        public synchronized void release() {
            held = false;
            this.notifyAll();
        }
        
        /** Waits for this lock to be released. */
        public synchronized void waitForRelease() throws InterruptedException {
            while (held) {
                this.wait();
            }
        }
    }
    
    private Map<String, TwoPLLock> lockTable;
    
    public TwoPLLocalLockManager() {
        lockTable = Maps.newConcurrentMap();
    }
    
    public synchronized boolean ownsLock(String key, long sessionId) {
        if (lockTable.containsKey(key)) {
            return lockTable.get(key).sessionId == sessionId;
        }
        return false;
    }
    
    /**
     * Locks the key for the given session, blocking as necessary.
     * Returns immediately if this session already has the lock.
     */
    public synchronized boolean lock(String key, long sessionId) {
        try {
            if (lockTable.containsKey(key)) {
                TwoPLLock lock = lockTable.get(key);
                if (lock.sessionId == sessionId) {
                    logger.debug("Lock re-granted for [" + sessionId + "] on key '" + key + "'");
                    return true;
                } else {
                    lock.waitForRelease();
                }
            }
            lockTable.put(key, TwoPLLock.createAndAcquire(sessionId));
            logger.debug("Lock granted for [" + sessionId + "] on key '" + key + "'");
            return true;
        } catch (InterruptedException e) {
            return false;
        }
    }
    
    /**
     * Returns true if there is no lock for the key after this action.
     * (i.e., it was removed or no lock existed.)
     * @throws IllegalArgumentException if we don't own the lock on the key.
     */
    public synchronized boolean unlock(String key, long sessionId) {
        if (lockTable.containsKey(key)) {
            TwoPLLock lock = lockTable.get(key);
            if (lock.sessionId == sessionId) {
                lock.release();
                lockTable.remove(key);
                logger.debug("Lock released by [" + sessionId + "] on key '" + key + "'");
                return true;
            } else {
                throw new IllegalArgumentException("[" + sessionId + "] cannot unlock key '" + key
                        + "', which is owned by [" + lock.sessionId + "]");
            }
        }
        return true;
    }
}
