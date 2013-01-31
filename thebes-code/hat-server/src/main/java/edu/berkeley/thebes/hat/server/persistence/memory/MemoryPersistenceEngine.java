package edu.berkeley.thebes.hat.server.persistence.memory;

import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.server.persistence.IPersistenceEngine;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MemoryPersistenceEngine implements IPersistenceEngine {
    private Map<String, DataItem> map;

    public void open() {
        map = new HashMap<String, DataItem>();
    }

    public boolean put(String key, DataItem value) {
        // If we already have this key, ensure new item has more recent timestamp
        if (map.containsKey(key)) {
            DataItem curItem = map.get(key);
            // TODO: What should we do in the = case?
            if (curItem.getTimestamp() >= value.getTimestamp()) {
                return false;
            }
        }
        
        // New key or newer timestamp.
        map.put(key, value);
        return true;
    }

    public DataItem get(String key) {
        return map.get(key);
    }

    public void close() {
        return;
    }
}