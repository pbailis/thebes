package edu.berkeley.thebes.hat.server.dependencies;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
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

public class DependencyResolver implements PendingWrite.WriteReadyCallback {
    
    private final AntiEntropyServiceRouter router;
    private final IPersistenceEngine persistenceEngine;
    private final ConcurrentMap<String, Set<PendingWrite>> pendingWritesMap;
    private final ConcurrentMap<String, Set<String>> unresolvedAcksMap;
    
    public DependencyResolver(AntiEntropyServiceRouter router,
            IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
        this.router = router;
        this.pendingWritesMap = Maps.newConcurrentMap();
        this.unresolvedAcksMap = Maps.newConcurrentMap();
    }

    public void addPendingWrite(String key, DataItem value) {
        pendingWritesMap.putIfAbsent(key, new ConcurrentSkipListSet<PendingWrite>());
        
        PendingWrite newPendingWrite = new PendingWrite(key, value, this); 
        pendingWritesMap.get(key).add(newPendingWrite);
        router.announcePendingWrite(newPendingWrite);
        // TODO: if it's still there after a while, can resend
        
        // Check any unresolved acks associated with this key
        if (unresolvedAcksMap.containsKey(key)) {
            Iterator<String> unresolvedAcksIterator = unresolvedAcksMap.get(key).iterator();
            while (unresolvedAcksIterator.hasNext()) {
                String ackedKey = unresolvedAcksIterator.next();
                if (informPendingWriteOfAck(newPendingWrite, ackedKey)) {
                    unresolvedAcksIterator.remove();
                }
            }
        }
    }
    
    public DataItem retrievePendingItem(String key, Version version) {
        for (PendingWrite pendingWrite : pendingWritesMap.get(key)) {
            DataItem value = pendingWrite.getValue();
            if (version.equals(value.getVersion())) {
                return value;
            }
        }
        return null;
    }
    
    @Override
    public void writeReady(PendingWrite write) {
        persistenceEngine.put(write.getKey(), write.getValue());
        pendingWritesMap.get(write.getKey()).remove(write);
    }
    
    public void dependentWriteAcked(String myKey, String ackedKey) {
        Set<PendingWrite> pendingWritesForKey = pendingWritesMap.get(myKey);
        for (PendingWrite pendingWrite : pendingWritesForKey) {
            if (informPendingWriteOfAck(pendingWrite, ackedKey)) {
                return;
            }
        }

        // No currently known PendingWrites wanted our ack!
        // Hopefully we'll soon have one that does, so keep it around.
        unresolvedAcksMap.putIfAbsent(myKey, new ConcurrentSkipListSet<String>());
        unresolvedAcksMap.get(myKey).add(ackedKey);
    }
    
    /** Returns true if the PendingWrite accepted our acked key! */
    private boolean informPendingWriteOfAck(PendingWrite pendingWrite, String ackedKey) {
        if (pendingWrite.isWaitingFor(ackedKey)) {
            pendingWrite.keyAcked(ackedKey);
            return true;
        }
        return false;
    }
}