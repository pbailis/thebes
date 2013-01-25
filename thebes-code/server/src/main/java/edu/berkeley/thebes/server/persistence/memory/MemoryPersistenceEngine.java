package edu.berkeley.thebes.server.persistence.memory;

import edu.berkeley.thebes.server.persistence.IPersistenceEngine;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class MemoryPersistenceEngine implements IPersistenceEngine {
    private Map<String, ByteBuffer> map;

    public void open() {
        map = new HashMap<String, ByteBuffer>();
    }

    public boolean put(String key, ByteBuffer value) {
        map.put(key, value);
        return true;
    }

    public ByteBuffer get(String key) {
        return map.get(key);
    }

    public void close() {
        return;
    }
}