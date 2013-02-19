package edu.berkeley.thebes.hat.server.dependencies;

import com.google.common.collect.Maps;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.common.thrift.Version;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class PendingWrites {
    /*
      Thrift datatypes have hashcode 0, making this annoying.
      However, the nested map structure makes GC pretty easy to remove
      items from pending when they are moved to good.
     */
    private Map<String, Map<Long, Map<Short, DataItem>>> pending;

    public PendingWrites() {
        pending = Maps.newHashMap();
    }

    public DataItem getMatchingItem(String key, Version version) {
        Map<Long, Map<Short, DataItem>> writesForKey;

        synchronized (pending) {
            writesForKey = pending.get(key);

            if(writesForKey == null)
                return null;
        }

        synchronized (writesForKey) {
            Map<Short, DataItem> writesWithStamp = writesForKey.get(version.getTimestamp());

            if(writesWithStamp == null)
                return null;

            synchronized(writesWithStamp) {
                DataItem pendingItem = writesWithStamp.get(version.getClientID());

                if(pendingItem == null)
                    return null;

                return pendingItem;
            }
        }
    }

    public void makeItemPending(String key, DataItem item) {
        synchronized (pending) {
            Map<Long, Map<Short, DataItem>> writesForKey = pending.get(key);

            if(writesForKey == null) {
                writesForKey = new HashMap<Long, Map<Short, DataItem>>();
                pending.put(key, writesForKey);
            }

            synchronized (writesForKey) {
                Map<Short, DataItem> writesWithStamp = writesForKey.get(item.getVersion().getTimestamp());

                if(writesWithStamp == null) {
                    writesWithStamp = Maps.newHashMap();
                    writesForKey.put(item.getVersion().getTimestamp(), writesWithStamp);
                }

                synchronized(writesWithStamp) {
                    writesWithStamp.put(item.getVersion().getClientID(), item);
                }
            }
        }
    }

    public void removeDominatedItems(String key, DataItem newItem) {
        Map<Long, Map<Short, DataItem>> writesForKey = pending.get(key);

        if(writesForKey == null)
            return;

        synchronized (writesForKey) {
            Iterator<Long> timestampIt = writesForKey.keySet().iterator();
            while(timestampIt.hasNext()) {
                long timestamp = timestampIt.next();
                if(timestamp < newItem.getVersion().getTimestamp()) {
                    timestampIt.remove();
                }
                else if(timestamp == newItem.getVersion().getTimestamp()) {
                    Map<Short, DataItem> writesWithStamp = writesForKey.get(newItem.getVersion().getTimestamp());

                    if(writesWithStamp != null) {
                        Iterator<Short> clientIDIt = writesWithStamp.keySet().iterator();
                        while(clientIDIt.hasNext()) {
                            short clientID = clientIDIt.next();
                            if(clientID <= newItem.getVersion().getClientID()) {
                                clientIDIt.remove();
                            }
                        }

                        if(writesWithStamp.keySet().isEmpty()) {
                            timestampIt.remove();
                        }
                    }
                }
            }

            if(writesForKey.keySet().isEmpty())
                pending.remove(key);
        }
    }
}