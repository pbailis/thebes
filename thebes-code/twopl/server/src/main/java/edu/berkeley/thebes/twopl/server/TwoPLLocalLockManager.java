package edu.berkeley.thebes.twopl.server;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Provides an interface for acquiring read and write locks.
 * Note that locks cannot be upgraded, so the caller must acquire the highest level of
 * lock needed initially, or else break 2PL.
 */
public class TwoPLLocalLockManager {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(TwoPLLocalLockManager.class);

    private final Counter lockMetric = Metrics.newCounter(TwoPLLocalLockManager.class, "2pl-locks");

    public enum LockType { READ, WRITE }
    
    /**
     * Contains all the state related to a particular locked object.
     * This class supports multiple readers xor a single writer.
     * 
     * Writers are given priority to dequeue before readers, and new readers cannot lock
     *   if there's a writer waiting.
     */
    private static class LockState {
    	public boolean held;
        public LockType mode;
        public Set<Long> lockers;
        private int numWritersWaiting;
        
        private Lock lockLock = new ReentrantLock();
        private Condition readersWaiting = lockLock.newCondition();
        private Condition writersWaiting = lockLock.newCondition();
        
        public LockState() {
            this.held = false;
            this.lockers = new ConcurrentSkipListSet<Long>();
            this.numWritersWaiting = 0;
        }
        
        private boolean shouldGrantLock(LockType wantType) {
            switch (wantType) {
            case READ:
                return (!held || mode == LockType.READ) && numWritersWaiting == 0;
            case WRITE:
                return !held;
            default:
                throw new IllegalArgumentException("Unknown lock type: " + wantType);
            }
        }
        
        public boolean acquire(LockType wantType, long sessionId) {
            lockLock.lock();
            try {
                while (!shouldGrantLock(wantType)) {
                    if (wantType == LockType.READ) {
                        readersWaiting.await();
                    } else if (wantType == LockType.WRITE) {
                        if (ownsLock(LockType.READ, sessionId)) {
                            throw new IllegalStateException("Cannot upgrade lock from READ TO WRITE for session " + sessionId);
                        }
                        
                        numWritersWaiting ++;
                        writersWaiting.await();
                        numWritersWaiting --;
                    }
                }

                // We own the lock-lock, let's go to town (and lock stuff)!
                this.held = true;
                this.lockers.add(sessionId);
                this.mode = wantType;
                return true;
            } catch (InterruptedException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            } finally {
                lockLock.unlock();
            }
        }
        
        public void release(long sessionId) {
            lockLock.lock();
            try {
                lockers.remove(sessionId);
                if (lockers.isEmpty()) {
                    held = false;
                    
                    if (numWritersWaiting > 0) {
                        writersWaiting.signal();
                    } else {
                        readersWaiting.signalAll();
                    }
                }
            } finally {
                lockLock.unlock();
            }
        }
        
        /** Returns true if the session owns the lock at the given level or above.
         * This method thus returns true if we own a WriteLock and want to READ. */
        public boolean ownsLock(LockType needType, long sessionId) {
            lockLock.lock();
            try {
                return ownsAnyLock(sessionId) &&
                        (needType == LockType.READ || mode == LockType.WRITE);
            } finally {
                lockLock.unlock();
            }
        }
        
        public boolean ownsAnyLock(long sessionId) {
            return lockers.contains(sessionId);
        }
    }
    
    private ConcurrentMap<String, LockState> lockTable;
    
    public TwoPLLocalLockManager() {
        lockTable = Maps.newConcurrentMap();
    }
    
    public boolean ownsLock(LockType type, String key, long sessionId) {
        if (lockTable.containsKey(key)) {
            return lockTable.get(key).ownsLock(type, sessionId);
        }
        return false;
    }
    
    /**
     * Locks the key for the given session, blocking as necessary.
     * Returns immediately if this session already has the lock.
     */
    // TODO: Consider using more performant code when logic settles down.
    // See: http://stackoverflow.com/a/13957003,
    //      and http://www.day.com/maven/jsr170/javadocs/jcr-2.0/javax/jcr/lock/LockManager.html
    public void lock(LockType lockType, String key, long sessionId) {
        
        lockTable.putIfAbsent(key, new LockState());
        LockState lockState = lockTable.get(key);
        
        if (lockState.ownsLock(lockType, sessionId)) {
            logger.debug(lockType + " Lock re-granted for [" + sessionId + "] on key '" + key + "'");
            return;
        }
        
        boolean acquired = lockState.acquire(lockType, sessionId);
        if (acquired) {
            lockMetric.inc();
            logger.debug(lockType + " Lock granted for [" + sessionId + "] on key '" + key + "'");
        } else {
            logger.error(lockType + " Lock unavailable for key '" + key + "'.");
            throw new IllegalStateException("Unable to acquire lock for key '" + key + "'.");
        }
    }
    
    /**
     * Returns true if there is no lock for the key after this action.
     * (i.e., it was removed or no lock existed.)
     * @throws IllegalArgumentException if we don't own the lock on the key.
     */
    public synchronized void unlock(String key, long sessionId) {
        if (lockTable.containsKey(key)) {
            LockState lockState = lockTable.get(key);
            if (lockState.ownsAnyLock(sessionId)) {
                lockState.release(sessionId);
                lockMetric.dec();
                logger.debug("Lock released by [" + sessionId + "] on key '" + key + "'");
            } else {
            	logger.error("These guys own it: " + lockState.lockers + " / " + lockState.held + " / " + lockState.mode);
                throw new IllegalArgumentException("[" + sessionId + "] cannot unlock key it does not own: '" + key + "'");
            }
        }
    }
}
