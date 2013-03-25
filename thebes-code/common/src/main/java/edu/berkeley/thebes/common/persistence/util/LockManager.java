package edu.berkeley.thebes.common.persistence.util;

import java.util.WeakHashMap;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockManager {
    private WeakHashMap<String, Condition> lockTable;
    private Lock tableLock = new ReentrantLock();

    public LockManager() {
        lockTable = new WeakHashMap<String, Condition>();
    }

    public void lock(String key) {
        tableLock.lock();
        if(!lockTable.containsKey(key))
            lockTable.put(key, tableLock.newCondition());
        else
            lockTable.get(key).awaitUninterruptibly();
        tableLock.unlock();
    }

    public synchronized void unlock(String key) {
        tableLock.lock();
        lockTable.get(key).signal();
        tableLock.unlock();
    }
}