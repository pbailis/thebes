package edu.berkeley.thebes.twopl.server;

import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides an interface for acquiring read and write locks.
 * Note that locks cannot be upgraded, so the caller must acquire the highest level of
 * lock needed initially, or else break 2PL.
 */
public class TwoPLLocalLockManager {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(TwoPLLocalLockManager.class);

    public enum LockType { READ, WRITE }
    
    /**
     * Contains all the state related to a particular locked object.
     * This class supports multiple readers xor a single writer.
     * 
     * Writers are given priority to dequeue before readers, and new readers cannot lock
     *   if there's a writer waiting.
     */
    private static class LockState {
        private boolean held;
        private LockType mode;
        private Set<Long> lockers;
        private int numWritersWaiting;
        
        public LockState() {
            this.held = false;
            this.lockers = new ConcurrentSkipListSet<Long>();
            this.numWritersWaiting = 0;
        }
        
        /**
         * Returns true if we acquire the lock successfully -- false otherwise.
         * Does not block.
         */
        public synchronized boolean acquire(LockType wantType, long sessionId) {
            boolean grantReadLock = 
                    wantType == LockType.READ && (!held || mode == LockType.READ);
            boolean grantWriteLock = (wantType == LockType.WRITE && !held); 
            if (grantReadLock || grantWriteLock) {
                this.held = true;
                this.lockers.add(sessionId);
                this.mode = wantType;
                return true;
            } else {
                return false;
            }
        }
        
        public synchronized void release(long sessionId) {
            lockers.remove(sessionId);
            if (lockers.isEmpty()) {
                held = false;
                this.notifyAll();
            }
        }
        
        public synchronized void waitForRelease(LockType wantType) throws InterruptedException {
            if (wantType == LockType.READ) {
                while ((held && mode == LockType.WRITE) || numWritersWaiting > 0) {
                    this.wait();
                }
            } else if (wantType == LockType.WRITE) {
                numWritersWaiting ++;
                while (held) {
                    this.wait();
                }
                numWritersWaiting --;
            }
        }
        
        /** Returns true if the session owns the lock at the given level or above.
         * This method thus returns true if we own a WriteLock and want to READ. */
        public synchronized boolean ownsLock(LockType needType, long sessionId) {
            return ownsAnyLock(sessionId) &&
                    (needType == LockType.READ || mode == LockType.WRITE);
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
    public boolean lock(LockType lockType, String key, long sessionId) {
        // Only attempt to lock a certain number of times.
        // Note that the only reason we should attempt to acquire a lock and fail is
        //   if a *new* lock request comes in at the same time as we try to acquire.
        //   This is extremely unlikely, so a number of failures indicates a likely bug.
        int attempts = 0;
        
        try {
            lockTable.putIfAbsent(key, new LockState());
            LockState lockState = lockTable.get(key);
            
            while (attempts++ < 10) {
                if (lockState.ownsLock(lockType, sessionId)) {
                    logger.debug(lockType + " Lock re-granted for [" + sessionId + "] on key '" + key + "'");
                    return true;
                } else {
                    lockState.waitForRelease(lockType);
                }
                
                boolean acquired = lockState.acquire(lockType, sessionId);
                if (acquired) {
                    logger.debug(lockType + " Lock granted for [" + sessionId + "] on key '" + key + "'");
                    return true;
                }
            }
            
            // Failed to acquire lock many times, something is up.
            logger.error(lockType + " Lock unavailable for key '" + key + "'.");
            return false;
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
            LockState lockState = lockTable.get(key);
            if (lockState.ownsAnyLock(sessionId)) {
                lockState.release(sessionId);
                logger.debug("Lock released by [" + sessionId + "] on key '" + key + "'");
                return true;
            } else {
                throw new IllegalArgumentException("[" + sessionId + "] cannot unlock key '" + key + "'");
            }
        }
        return true;
    }
}
