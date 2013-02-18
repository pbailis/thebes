package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.Maps;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/* Handles all logic for incoming causal dependency check requests
   This includes buffering requests and synchronizing new value updates */
public class GenericDependencyResolver {

    private class DependencyWaitingQueue {
        private long lastSeenVersion;
        private int numWaiters;

        public DependencyWaitingQueue(long lastSeenVersion) {
            this.lastSeenVersion = lastSeenVersion;
        }

        public DependencyWaitingQueue() {
            this(-1);
        }

        public synchronized void setLastSeenVersion(long newVersion) {
            if(newVersion > lastSeenVersion)
                this.lastSeenVersion = newVersion;
        }

        public synchronized void prepareToBlockForDependency() {
            numWaiters++;
        }

        public synchronized void blockForDependency(DataDependency dependency) {
            while(true) {
                try {
                    if(lastSeenVersion >= dependency.getTimestamp())
                        return;
                    this.wait();
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage());
                }
            }
        }

        public boolean isEmpty() {
            return numWaiters == 0;
        }
    }

    private Map<String, DependencyWaitingQueue> blocked;
    private Lock blockedLock = new ReentrantLock();

    private IPersistenceEngine engine;
    private static Logger logger = LoggerFactory.getLogger(
            GenericDependencyResolver.class);


    public GenericDependencyResolver(IPersistenceEngine engine) {
        this.engine = engine;
        blocked = Maps.newHashMap();
    }

    public void blockForDependency(DataDependency dependency) {
        DataItem storedItem = engine.get(dependency.getKey());
        if(storedItem != null
           && storedItem.getTimestamp() >= dependency.getTimestamp()) {
            blockedLock.lock();
            DependencyWaitingQueue queue = blocked.get(dependency.getKey());
            if(queue != null && queue.isEmpty()) {
                blocked.remove(dependency.getKey());
            }
            blockedLock.unlock();
            return;
        }

        blockedLock.lock();

        DependencyWaitingQueue queue = blocked.get(dependency.getKey());
        if(queue == null) {
            queue = new DependencyWaitingQueue();
            blocked.put(dependency.getKey(), queue);
        }

        queue.prepareToBlockForDependency();
        blockedLock.unlock();
        queue.blockForDependency(dependency);
    }

    public void notifyNewLocalWrite(String key, DataItem newWrite) {
        blockedLock.lock();
        DependencyWaitingQueue waiting = blocked.get(key);
        blockedLock.unlock();

        if(waiting == null)
            return;

        waiting.setLastSeenVersion(newWrite.getTimestamp());

        synchronized (waiting) {
            waiting.notifyAll();
        }
    }
}