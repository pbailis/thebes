package edu.berkeley.thebes.hat.server.dependencies;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;

public class PendingWrites {
    private ConcurrentMap<String, ConcurrentLinkedQueue<DataItem>> pending;
    /** Class lock. Monotonic operations acquire the read lock (multiple may happen simultaneously),
     * while deletion operations acquire the write lock to ensure no races. */
    private ReadWriteLock monotonicityLock = new ReentrantReadWriteLock();

    private final Counter pendingWritesCounter = Metrics.newCounter(PendingWrites.class, "pendingWrites");

    public DataItem getMatchingItem(String key, Version version) {
	monotonicityLock.readLock().lock();

	try {
	    if (!pending.containsKey(key)) {
		return null;
	    }

	    //todo: make more efficient lookup, here it's O(n)
	    // (aaron): on versions for the same key... expecting lots?
	    ConcurrentLinkedQueue<DataItem> pendingList = pending.get(key);

	    if (pendingList == null) {
		return null;
	    }

	    for (DataItem pendingWrite : pendingList.iterator()) {
		if (pendingWrite.getVersion().equals(version)) {
		    return pendingWrite;
		}
	    }

	    return null;
	} finally {
	    monotonicityLock.readLock().unlock();
	}
    }

    public void makeItemPending(String key, DataItem item) {
	monotonicityLock.readLock().lock();
	
	try {
	    pending.putIfAbsent(key, new ConcurrentLinkedQueue<DataItem>());

	    //todo: race condition?
	    ConcurrentLinkedQueue<DataItem> pendingList = pending.get(key);
	    pendingList.add(item);
	    pendingWritesCounter.inc();
	} finally {
	    monotonicityLock.readLock().unlock();
    }

    public void removeDominatedItems(String key, DataItem newItem) {
	monotonicityLock.writeLock().lock();

	try {
	    if (!pending.containsKey(key)) {
		return;
	    }

	    ConcurrentLinkedQueue<DataItem> pendingList = pending.get(key);

	    if(pendingList == null)
		return;

	    Iterator<DataItem> pendingIt = pendingList.iterator();

	    while(pendingIt.hasNext()) {
		DataItem pendingWrite = pendingIt.next();
		if(pendingWrite.getVersion().compareTo(newItem.getVersion()) <= 0) {
		    pendingIt.remove();
		    pendingWritesCounter.dec();
		}
	    }

	    if(pendingList.size() == 0) {
		pending.remove(key);
	    }
	} finally {
	    monotonicityLock.writeLock().unlock();
	}
    }
}