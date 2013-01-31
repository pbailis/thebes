package edu.berkeley.thebes.hat.server.persistence.memory;

import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.server.persistence.IPersistenceEngine;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;

public class MemoryPersistenceEngine implements IPersistenceEngine {
    private Map<String, DataItem> map;

    public void open() {
        map = Maps.newHashMap();
    }

    /**
     * Puts the given value for our key.
     * Does not update the value if the key already exists with a later timestamp.
     */
    public boolean put(String key, DataItem value) {
        // If we already have this key, ensure new item has more recent timestamp
        if (map.containsKey(key)) {
            DataItem curItem = map.get(key);
            if (curItem.getTimestamp() > value.getTimestamp()) {
                return false;
            } else if (curItem.getTimestamp() == value.getTimestamp()) {
                // If the timestamps are equal, just order by the data bytes.
                Comparator<byte[]> byteComparator = UnsignedBytes.lexicographicalComparator();
                
                if (byteComparator.compare(curItem.getData(), value.getData()) >= 0) {
                    return false;
                }
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