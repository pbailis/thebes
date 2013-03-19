package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;

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
    private final ConcurrentMap<String, Set<Ack>> unresolvedAcksMap;
    
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
            Iterator<Ack> unresolvedAcksIterator = unresolvedAcksMap.get(key).iterator();
            while (unresolvedAcksIterator.hasNext()) {
                Ack ack = unresolvedAcksIterator.next();
                if (informPendingWriteOfAck(newPendingWrite, ack)) {
                    unresolvedAcksIterator.remove();
                }
            }
        }
    }
    
    public DataItem retrievePendingItem(String key, Version version) {
        if (!pendingWritesMap.containsKey(key)) {
            return null;
        }
        
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
    
    public void dependentWriteAcked(String myKey, String ackedKey, Version version) {
        Ack ack = new Ack(ackedKey, version);
        
        Set<PendingWrite> pendingWritesForKey = pendingWritesMap.get(myKey);
        if (pendingWritesForKey != null) {
            for (PendingWrite pendingWrite : pendingWritesForKey) {
                if (informPendingWriteOfAck(pendingWrite, ack)) {
                    return;
                }
            }
        }

        // No currently known PendingWrites wanted our ack!
        // Hopefully we'll soon have one that does, so keep it around.
        unresolvedAcksMap.putIfAbsent(myKey, new ConcurrentSkipListSet<Ack>());
        unresolvedAcksMap.get(myKey).add(ack);
    }
    
    /** Returns true if the PendingWrite accepted our acked key! */
    private boolean informPendingWriteOfAck(PendingWrite pendingWrite, Ack ack) {
        if (pendingWrite.isWaitingFor(ack.key, ack.version)) {
            pendingWrite.keyAcked(ack.key);
            return true;
        }
        return false;
    }
    
    private static class Ack implements Comparable<Ack> {
        public String key;
        public Version version;
        
        public Ack(String ackedKey, Version version) {
            this.key = ackedKey;
            this.version = version;
        }

        @Override
        public int compareTo(Ack o) {
            return ComparisonChain.start()
                    .compare(key, o.key)
                    .compare(version, o.version)
                    .result();
        } 
    }
}