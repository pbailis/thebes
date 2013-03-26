package edu.berkeley.thebes.hat.server.dependencies;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicBoolean;
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
    private static Logger logger = LoggerFactory.getLogger(DependencyResolver.class);
    
    private final AntiEntropyServiceRouter router;
    private final IPersistenceEngine persistenceEngine;
    private final ConcurrentMap<Version, TransactionQueue> pendingTransactionsMap;
    private final ConcurrentMap<Version, AtomicInteger> unresolvedAcksMap;
    
    private final Lock unresolvedAcksLock;
    
    public DependencyResolver(AntiEntropyServiceRouter router,
            IPersistenceEngine persistenceEngine) {
        this.persistenceEngine = persistenceEngine;
        this.router = router;
        this.pendingTransactionsMap = Maps.newConcurrentMap();
        this.unresolvedAcksMap = Maps.newConcurrentMap();
        this.unresolvedAcksLock = new ReentrantLock();
    }

    public void addPendingWrite(String key, DataItem value) throws TException {
        Version version = value.getVersion();
        
        pendingTransactionsMap.putIfAbsent(version, new TransactionQueue(version));
        
        PendingWrite newPendingWrite = new PendingWrite(key, value);
        
        TransactionQueue transQueue = pendingTransactionsMap.get(version);
        transQueue.add(newPendingWrite);
        
        if (transQueue.shouldAnnounceTransactionReady()) {
            router.announceTransactionReady(version, transQueue.replicaIndicesInvolved);
        }
        // TODO: if it's still there after a while, can resend
        
        // Check any unresolved acks associated with this key
        // TODO: Examine the implications of this!
        unresolvedAcksLock.lock();
        boolean shouldCommit = false;
        try {
            if (unresolvedAcksMap.containsKey(version)) {
                AtomicInteger numAcksForTransaction = unresolvedAcksMap.get(version);
                for (int i = 0; i < numAcksForTransaction.get() && !shouldCommit; i ++) {
                    shouldCommit = transQueue.serverAcked();
                }
                unresolvedAcksMap.remove(version);
            }
        } finally {
            unresolvedAcksLock.unlock();
        }
        
        if (shouldCommit) {
            commit(transQueue);
        }
        
//        if (Math.random() < .001) {
//            int numPendingWrites = 0;
//            for (Set<PendingWrite> pendingWrites : pendingWritesMap.values()) {
//                numPendingWrites += pendingWrites.size();
//            }
//            logger.debug("Currently have " + numPendingWrites + " pending writes!");
//        }
    }
    
    private void commit(TransactionQueue queue) throws TException {
        for (PendingWrite write : queue.pendingWrites) {
            persistenceEngine.put(write.getKey(), write.getValue());
        }
        pendingTransactionsMap.remove(queue.version);
    }
    
    public DataItem retrievePendingItem(String key, Version version) {
        if (!pendingTransactionsMap.containsKey(version)) {
            return null;
        }
        
        for (PendingWrite pendingWrite : pendingTransactionsMap.get(version).pendingWrites) {
            if (key.equals(pendingWrite.getKey())) {
                return pendingWrite.getValue();
            }
        }
        
        return null;
    }

    public void ackTransactionPending(Version transactionId) throws TException {
        TransactionQueue transactionQueue = pendingTransactionsMap.get(transactionId);
        if (transactionQueue != null && transactionQueue.serverAcked()) {
            commit(transactionQueue);
            return;
        }
        
        // No currently known PendingWrites wanted our ack!
        // Hopefully we'll soon have one that does, so keep it around.
        unresolvedAcksLock.lock();
        try {
            unresolvedAcksMap.putIfAbsent(transactionId, new AtomicInteger(0));
            unresolvedAcksMap.get(transactionId).incrementAndGet();
        } finally {
            unresolvedAcksLock.unlock();
        }
    }
    
    private static class TransactionQueue {
        private final Version version;
        private int numKeysForThisReplica;
        private int numReplicasInvolved; 
        private Set<Integer> replicaIndicesInvolved;
        private final Set<PendingWrite> pendingWrites;
        private AtomicBoolean alreadySentAnnouncement = new AtomicBoolean(false);
        private AtomicInteger numReplicasAcked;
        
        public TransactionQueue(Version version) {
            this.version = version;
            this.pendingWrites = new ConcurrentSkipListSet<PendingWrite>();
            this.numReplicasAcked = new AtomicInteger(0);
        }
        
        public void add(PendingWrite write) {
            pendingWrites.add(write);
            if (!(numKeysForThisReplica == 0 ||
                    numKeysForThisReplica == write.getNumKeysForThisReplica())) {
                logger.error(String.format("numReplicasInvolved is %d, replicaIndicesInvolved is %d, key is %s, version is %s, txn keys are %s", numReplicasInvolved, write.getReplicaIndicesInvolved().size(), write.getKey(), write.getVersion(), write.getValue().getTransactionKeys()));
                assert(false);
            }
            if (!(numReplicasInvolved == 0 ||
                    numReplicasInvolved == write.getReplicaIndicesInvolved().size())) {
                logger.error(String.format("numReplicasInvolved is %d, replicaIndicesInvolved is %d, key is %s, version is %s, txn keys are %s", numReplicasInvolved, write.getReplicaIndicesInvolved().size(), write.getKey(), write.getVersion(), write.getValue().getTransactionKeys()));
                assert(false);
            }
            
            this.numKeysForThisReplica = write.getNumKeysForThisReplica();
            this.numReplicasInvolved = write.getReplicaIndicesInvolved().size();
            this.replicaIndicesInvolved = write.getReplicaIndicesInvolved();
        }
        
        public boolean serverAcked() {
            return numReplicasAcked.incrementAndGet() == numReplicasInvolved; 
        }
        
        public boolean shouldAnnounceTransactionReady() {
            return pendingWrites.size() == numKeysForThisReplica
                    && !alreadySentAnnouncement.getAndSet(true);
        }
    }
}