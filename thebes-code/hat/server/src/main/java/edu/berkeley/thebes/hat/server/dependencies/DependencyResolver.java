package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftUtil;
import edu.berkeley.thebes.common.thrift.ThriftUtil.VersionCompare;
import edu.berkeley.thebes.common.thrift.Version;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/*
The following class does most of the heavy lifting for the HAT
partial ordering (causality and transactional atomicity). The
basic idea is that each write's causal dependencies should be
present in the cluster before it is applied locally, and each
transaction's sibling dependencies should be present in
PendingWrites on their respective nodes before the write is
applied.

A causal dependency is satisfied when there is a write to the
appropriate key in the local persistence engine with a timestamp
greater than or equal to that of the causal dependency .

A transactional atomicity dependency is satisfied when there is a
write to the appropriate key in the local persistence engine with
a timestamp greater than or equal to that of the causal
dependency *or* a write in PendingWrites with an *exact* match
for a timestamp.

Before applying an incoming write, dependencies need to be
checked. For a write originating in the local cluster, we know
that the causal dependencies are already present, so we do not
check them and instead only check the transactional atomicity
dependencies. For a write originating from a remote cluster, we
check both causal dependencies and transactional atomicity
dependencies.  Writes are only placed in PendingWrites once their
causal dependencies are handled.

To check a local server in a cluster for these dependencies, a
server calls AntiEntropyService.waitFor{Causal,
Transactional}Dependency. This in turn calls
DependencyResolver.blockForDependency(). A thread in
blockForDependency queries the local persistence engine and/or
the PendingWrites list as necessary, waiting until an appropriate
dependency is found.  waitForCausalDependency() returns when the
write is in the persistence engine, while
waitForTransactionalDependency() returns when the write is (at
least) in PendingWrites.

Every time a new local write is applied, calls to
blockForDependency() need to be notified and so
notifyNewLocalWrite() is called.
*/

public class DependencyResolver {

    private class DependencyWaitingQueue {
        private Version lastWrittenVersion;
        private int numWaiters;

        public DependencyWaitingQueue(Version lastWrittenVersion) {

            this.lastWrittenVersion = lastWrittenVersion;
        }

        public DependencyWaitingQueue() {
            this(ThriftUtil.NullVersion);
        }

        public synchronized void setLastWrittenVersion(Version newVersion) {
            if(ThriftUtil.compareVersions(newVersion, lastWrittenVersion) == VersionCompare.LATER)
                this.lastWrittenVersion = newVersion;
        }

        public synchronized void prepareToBlockForDependency() {
            numWaiters++;
        }

        public synchronized void blockForDependency(DataDependency dependency, DependencyType dependencyType) {
            while(true) {
                try {
                    if((ThriftUtil.compareVersions(lastWrittenVersion, dependency.getVersion()) != VersionCompare.EARLIER) ||
                       (dependencyType == DependencyType.ATOMIC &&
                        pendingWrites.getMatchingItem(dependency.getKey(), dependency.getVersion()) != null)) {
                        numWaiters--;
                        return;
                    }

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

    private class CausalDependencyCallback implements
        AsyncMethodCallback<AntiEntropyService.AsyncClient.waitForCausalDependency_call> {
        private DependencyCallback callback;

        public CausalDependencyCallback(String key, AtomicInteger blockFor) {
            callback = new DependencyCallback(key, blockFor);
        }

        @Override
        public void onComplete(AntiEntropyService.AsyncClient.waitForCausalDependency_call waitForDependency_call) {
            callback.onComplete();
        }

        @Override
        public void onError(Exception e) {
            callback.onError(e);
        }
    }

    private class TransactionalDependencyCallback implements
        AsyncMethodCallback<AntiEntropyService.AsyncClient.waitForTransactionalDependency_call> {
        private DependencyCallback callback;

        public TransactionalDependencyCallback(String key, AtomicInteger blockFor) {
            callback = new DependencyCallback(key, blockFor);
        }

        @Override
        public void onComplete(AntiEntropyService.AsyncClient.waitForTransactionalDependency_call waitForDependency_call) {
            callback.onComplete();
        }

        @Override
        public void onError(Exception e) {
            callback.onError(e);
        }
    }

    private class DependencyCallback {
        String key;
        AtomicInteger blockFor;

        public DependencyCallback(String key,
                                  AtomicInteger blockFor) {
            this.key = key;
            this.blockFor = blockFor;
        }

        public void onComplete() {
            if(blockFor.decrementAndGet() == 0) {
                blockFor.notify();
            }
        }

        public void onError(Exception e) {
            logger.warn(e.getMessage());
            // todo: rethink this behavior
            onComplete();
        }
    }

    private static List<DataDependency> keyListToDependencyList(List<String> dependencyKeys, Version version) {
        List<DataDependency> ret = Lists.newArrayList();
        for(String key : dependencyKeys) {
            ret.add(new DataDependency(key, version));
        }

        return ret;
    }

    private enum DependencyType { CAUSAL, ATOMIC };

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

    public void blockForCausalDependency(DataDependency dependency) {
        blockForDependency(dependency,  DependencyType.CAUSAL);
    }

    public void blockForAtomicDependency(DataDependency dependency) {
        blockForDependency(dependency,  DependencyType.ATOMIC);
    }

    private void blockForDependency(DataDependency dependency, DependencyType dependencyType) {
        DataItem storedItem = persistenceEngine.get(dependency.getKey());
        if((storedItem != null
           && ThriftUtil.compareVersions(storedItem.getVersion(), dependency.getVersion()) == VersionCompare.EARLIER) ||
                dependencyType == DependencyType.ATOMIC &&
                pendingWrites.getMatchingItem(dependency.getKey(), dependency.getVersion()) != null) {
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
        queue.blockForDependency(dependency, dependencyType);
    }

    private boolean resolveDependencies(String key,
                                        List<DataDependency> dependencies,
                                        DependencyType dependencyType) throws TException {
        if(dependencies.size() > 0) {
            AtomicInteger waiting = new AtomicInteger(dependencies.size());

            synchronized (waiting) {
                for(DataDependency dependency : dependencies) {
                    AntiEntropyService.AsyncClient client = router.getReplicaByKey(dependency.getKey());

                    // technically don't need a callback per call, but the state is in 'waiting' anyway
                    if(dependencyType == DependencyType.CAUSAL) {
                        client.waitForCausalDependency(dependency, new CausalDependencyCallback(key, waiting));
                    }
                    else if (dependencyType == DependencyType.ATOMIC) {
                        client.waitForTransactionalDependency(dependency, new TransactionalDependencyCallback(key, waiting));
                    }
                }

                try {
                    waiting.wait();
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                    return false;
                }
            }
        }

        return true;
    }

    public void asyncApplyNewLocalWrite(final String key,
                                        final DataItem write,
                                        final List<String> transactionKeys) throws TException {
        asyncApplyNewWrite(key,
                           write,
                           new ArrayList<DataDependency>(),
                           keyListToDependencyList(transactionKeys, write.getVersion()),
                           true);
    }

    public void asyncApplyNewRemoteWrite(final String key,
                                        final DataItem write,
                                        final List<DataDependency> causalDependencies,
                                        final List<String> transactionKeys) throws TException {
        asyncApplyNewWrite(key,
                           write,
                           causalDependencies,
                           keyListToDependencyList(transactionKeys, write.getVersion()),
                           false);
    }

    private void asyncApplyNewWrite(final String key,
                                   final DataItem write,
                                   final List<DataDependency> causalDependencies,
                                   final List<DataDependency> atomicityDependencies,
                                   boolean local) throws TException {

        if(local && atomicityDependencies.isEmpty()) {
            persistenceEngine.put(key, write);
            notifyNewLocalWrite(key, write);
            return;
        }

        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    if(ThriftUtil.compareVersions(persistenceEngine.get(key).getVersion(), write.getVersion()) != VersionCompare.EARLIER)
                        return;

                    if(!resolveDependencies(key, causalDependencies, DependencyType.CAUSAL)) {
                        return;
                    }

                    if(!atomicityDependencies.isEmpty())
                        pendingWrites.makeItemPending(key, write);

                    if(!resolveDependencies(key, atomicityDependencies, DependencyType.ATOMIC))
                        return;

                    persistenceEngine.put(key, write);
                    notifyNewLocalWrite(key, write);
                } catch (Exception e) {
                    logger.warn(e.getMessage());
                }
            }
        } ).start();
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