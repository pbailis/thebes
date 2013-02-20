package edu.berkeley.thebes.hat.server.dependencies;

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;

public class PendingWrites {
    private ConcurrentMap<String, ConcurrentLinkedQueue<DataItem>> pending;

    public PendingWrites() {
        pending = Maps.newConcurrentMap();
        //todo: FIX THIS GAUGE
        Metrics.newGauge(PendingWrites.class, "hat-pending", "numpending",
    		new Gauge<Integer>() {
    			@Override
    			public Integer value() {
    				synchronized (pending) {
    					return pending.size();
    				}
    			}});
    }

    public DataItem getMatchingItem(String key, Version version) {
        if(!pending.containsKey(key)) {
            return null;
        }

        //todo: make more efficient lookup, here it's O(n)
        ConcurrentLinkedQueue<DataItem> pendingList = pending.get(key);

        if(pendingList == null)
            return null;

        Iterator<DataItem> pendingIt = pendingList.iterator();

        while(pendingIt.hasNext()) {
            DataItem pendingWrite = pendingIt.next();
            if(pendingWrite.getVersion().equals(version)) {
                return pendingWrite;
            }
        }

        return null;
    }

    public void makeItemPending(String key, DataItem item) {
        pending.putIfAbsent(key, new ConcurrentLinkedQueue<DataItem>());

        //todo: race condition?
        ConcurrentLinkedQueue<DataItem> pendingList = pending.get(key);
        pendingList.add(item);
    }

    public void removeDominatedItems(String key, DataItem newItem) {
        if(!pending.containsKey(key)) {
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
            }
        }

        if(pendingList.size() == 0) {
            pending.remove(key);
        }
    }
}