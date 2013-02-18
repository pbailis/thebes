package edu.berkeley.thebes.hat.server.causal;

import com.google.common.collect.Maps;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/* Handles all logic for incoming causal dependency check requests
   This includes buffering requests and synchronizing new value updates */
public class CausalDependencyResolver {

    private Map<String, AtomicInteger> blocked;
    private Lock blockedLock = new ReentrantLock();

    private IPersistenceEngine engine;
    private static Logger logger = LoggerFactory.getLogger(CausalDependencyResolver.class);


    public CausalDependencyResolver(IPersistenceEngine engine) {
        this.engine = engine;
        blocked = Maps.newHashMap();
    }

    public void blockForDependency(DataDependency dependency) {
        AtomicInteger numWaiting = null;

        while(true) {
            DataItem storedItem = engine.get(dependency.getKey());
            if(storedItem != null
               && storedItem.getTimestamp() >= dependency.getTimestamp()) {

                blockedLock.lock();
                if(numWaiting != null && numWaiting == 0) {
                    blocked.remove(dependency.getKey());
                }
                blockedLock.unlock();
                return;
            }

            blockedLock.lock();

            numWaiting = blocked.get(dependency.getKey());
            if(numWaiting == null) {
                numWaiting = new AtomicInteger(0);
                blocked.put(dependency.getKey(), numWaiting);
            }
            else{
                numWaiting = blocked.get(dependency.getKey());
            }

            synchronized (numWaiting) {
                numWaiting.incrementAndGet();
                blockedLock.unlock();
                try {
                    numWaiting.wait();
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage());
                }
            }
        }
    }

    public void notifyNewLocalWrite(String key) {
        blockedLock.lock();
        Integer waiting = blocked.get(key);
        blockedLock.unlock();

        if(waiting == null)
            return;

        synchronized (waiting) {
            waiting.notifyAll();
        }
    }
}