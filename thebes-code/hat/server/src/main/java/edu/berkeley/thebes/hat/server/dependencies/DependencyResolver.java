package edu.berkeley.thebes.hat.server.dependencies;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;

/*
The following class does most of the heavy lifting for the HAT
partial ordering (causality and transactional atomicity). The
basic idea is that each write's causal dependencies should be
present in the cluster before it is applied locally, and each
transaction's sibling dependencies should be present in
PendingWrites on their respective nodes before the write is
applied.

A transactional atomicity dependency is satisfied when there is a
write to the appropriate key in the local persistence engine with
a timestamp greater than or equal to that of the causal
dependency *or* a write in PendingWrites with an *exact* match
for a timestamp.

To check a local server in a cluster for these dependencies, a
server calls AntiEntropyService.waitForTransactionalDependency. This in turn calls
DependencyResolver.waitInQueueForDependency(). A thread in
waitInQueueForDependency queries the local persistence engine and/or
the PendingWrites list as necessary, waiting until an appropriate
dependency is found.  waitForCausalDependency() returns when the
write is in the persistence engine, while
waitForTransactionalDependency() returns when the write is (at
least) in PendingWrites.

Every time a new local write is applied, calls to
waitInQueueForDependency() need to be notified and so
notifyNewLocalWrite() is called.
*/

public class DependencyResolver {

    private final Timer waitingDependencyTimerMetric = Metrics.newTimer(DependencyResolver.class,
                                                                        "hat-dependencies-check-waiting",
                                                                        TimeUnit.MILLISECONDS,
                                                                        TimeUnit.SECONDS);

    private final Timer resolvingDependencyTimerMetric = Metrics.newTimer(DependencyResolver.class,
                                                                          "hat-dependencies-resolving",
                                                                          TimeUnit.MILLISECONDS,
                                                                          TimeUnit.SECONDS);

    private final Timer resolvingAtomicDependencyTimerMetric = Metrics.newTimer(DependencyResolver.class,
                                                                          "hat-dependencies-resolving-atomic",
                                                                          TimeUnit.MILLISECONDS,
                                                                          TimeUnit.SECONDS);

    private class DependencyWaitingQueue {
        private Version lastWrittenVersion;
        private int numWaiters;

        public DependencyWaitingQueue(Version lastWrittenVersion) {
            this.lastWrittenVersion = lastWrittenVersion;
        }

        public DependencyWaitingQueue() {
            this(Version.NULL_VERSION);
        }

        public synchronized void setLastWrittenVersion(Version newVersion) {
            if (newVersion.compareTo(lastWrittenVersion) > 0)
                this.lastWrittenVersion = newVersion;
        }

        public synchronized void incrementQueueReferenceCount() {
            numWaiters++;
        }

        public synchronized void waitInQueueForDependency(DataDependency dependency) {
            TimerContext startTime = waitingDependencyTimerMetric.time();
            while(true) {
                try {
                    if (lastWrittenVersion.compareTo(dependency.getVersion()) >= 0 ||
                    		pendingWrites.getMatchingItem(dependency.getKey(), dependency.getVersion()) != null) {
                        numWaiters--;
                        startTime.stop();
                        return;
                    }

                    this.wait();
                } catch (InterruptedException e) {
                    logger.warn("error:", e);
                }
            }
        }

        public boolean isEmpty() {
            return numWaiters == 0;
        }
    }

    private Map<String, DependencyWaitingQueue> blocked = Maps.newHashMap();
    private Lock blockedLock = new ReentrantLock();

    private IPersistenceEngine persistenceEngine;
    private PendingWrites pendingWrites;
    private static Logger logger = LoggerFactory.getLogger(DependencyResolver.class);

    private AntiEntropyServiceRouter router;

    public DependencyResolver(IPersistenceEngine persistenceEngine,
                              PendingWrites pendingWrites,
                              AntiEntropyServiceRouter router) {
        this.persistenceEngine = persistenceEngine;
        this.pendingWrites = pendingWrites;
        this.router = router;
    }

    private void removePossiblyEmptyQueueByKey(String key) {
        blockedLock.lock();
        DependencyWaitingQueue queue = blocked.get(key);
        if(queue != null && queue.isEmpty()) {
            blocked.remove(key);
        }
        blockedLock.unlock();
    }

    public void blockForAtomicDependency(DataDependency dependency) {
        DataItem storedItem = persistenceEngine.get(dependency.getKey());
        
        if((storedItem != null && storedItem.getVersion().compareTo(dependency.getVersion()) >= 0) ||
                (pendingWrites.getMatchingItem(dependency.getKey(), dependency.getVersion()) != null)) {
                 removePossiblyEmptyQueueByKey(dependency.getKey());
            return;
        }

        blockedLock.lock();

        DependencyWaitingQueue queue = blocked.get(dependency.getKey());
        if(queue == null) {
            queue = new DependencyWaitingQueue();
            blocked.put(dependency.getKey(), queue);
        }

        queue.incrementQueueReferenceCount();
        blockedLock.unlock();
        queue.waitInQueueForDependency(dependency);
        removePossiblyEmptyQueueByKey(dependency.getKey());
    }

    public class WaitingResolvedDependency {
        private String key;
        private DataItem write;
        private AtomicInteger waitingCount;

        public WaitingResolvedDependency(String key,
                                         DataItem write,
                                         AtomicInteger waitingCount) {
            this.key = key;
            this.write = write;
            this.waitingCount = waitingCount;
        }

        public void notifyResolved() {
            logger.debug("Resolved!: " + waitingCount.get());
            if(this.waitingCount.decrementAndGet() == 0) {
                persistenceEngine.put(key, write);
                notifyNewLocalWrite(key, write);
            }
        }
    }

    private void asyncResolveDependencies(String key,
                                          DataItem write) throws TException {
        if(write.getTransactionKeys().size() > 0) {
            AtomicInteger waitCount = new AtomicInteger(write.getTransactionKeys().size());

            for(String atomicKey : write.getTransactionKeys()) {
                if (atomicKey.equals(key)) {
                    logger.debug("Waiting on self");
                }
                router.waitForDependencyRemote(key,
                                               new DataDependency(atomicKey, write.getVersion()),
                                               new WaitingResolvedDependency(key,
                                                                             write,
                                                                             waitCount));
            }
        }
    }

    public void asyncApplyNewWrite(final String key,
                                   final DataItem write) throws TException {

        if(write.getTransactionKeys() == null || write.getTransactionKeys().isEmpty()) {
            logger.debug("Autoresolve!");
            persistenceEngine.put(key, write);
            notifyNewLocalWrite(key, write);
            return;
        }

        if(persistenceEngine.get(key).getVersion().compareTo(write.getVersion()) >= 0)
            return;

        pendingWrites.makeItemPending(key, write);

        asyncResolveDependencies(key, write);
    }

    public void notifyNewLocalWrite(String key, DataItem newWrite) {
        blockedLock.lock();
        DependencyWaitingQueue waiting = blocked.get(key);
        blockedLock.unlock();

        if(waiting == null)
            return;

        waiting.setLastWrittenVersion(newWrite.getVersion());

        synchronized (waiting) {
            waiting.notifyAll();
        }

        pendingWrites.removeDominatedItems(key, newWrite);
    }
}