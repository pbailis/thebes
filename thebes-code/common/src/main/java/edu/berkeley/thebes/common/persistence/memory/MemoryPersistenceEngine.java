package edu.berkeley.thebes.common.persistence.memory;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Meter;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftUtil;
import edu.berkeley.thebes.common.thrift.ThriftUtil.VersionCompare;
import edu.berkeley.thebes.common.thrift.Version;

public class MemoryPersistenceEngine implements IPersistenceEngine {
    private final Meter putsMetric = Metrics.newMeter(MemoryPersistenceEngine.class, "put-requests", "requests", TimeUnit.SECONDS);
    private final Meter getsMetric = Metrics.newMeter(MemoryPersistenceEngine.class, "get-requests", "requests", TimeUnit.SECONDS);

    private Map<String, DataItem> map;
    private DataItem nullItem = new DataItem(ByteBuffer.allocate(0), ThriftUtil.NullVersion);

    public void open() {
        map = Maps.newConcurrentMap();
        Metrics.newGauge(MemoryPersistenceEngine.class, "num-keys", new Gauge<Integer>() {
            @Override
            public Integer value() {
                return map.size();
            }
        });
    }

    /**
     * Puts the given value for our key.
     * Does not update the value if the key already exists with a later timestamp.
     */
    public boolean put(String key, DataItem value) {
        putsMetric.mark();
        synchronized (map) {
            // If we already have this key, ensure new item has more recent timestamp
            if (map.containsKey(key)) {
                DataItem curItem = map.get(key);
                if (ThriftUtil.compareVersions(curItem.getVersion(), value.getVersion()) != VersionCompare.LATER)
                    return false;
            }

            // New key or newer timestamp.
            map.put(key, value);
        }

        return true;
    }

    public DataItem get(String key) {
        getsMetric.mark();
        DataItem ret = map.get(key);
        if(ret == null)
            ret = nullItem;

        return ret;
    }

    public void close() {
        return;
    }
}