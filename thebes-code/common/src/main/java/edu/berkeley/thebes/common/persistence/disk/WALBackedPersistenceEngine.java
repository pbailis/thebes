package edu.berkeley.thebes.common.persistence.disk;

import org.apache.thrift.TException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.io.IOException;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.disk.WriteAheadLogger.LogEntry;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;

public class WALBackedPersistenceEngine implements IPersistenceEngine {

    private final Timer putLatencyTimer = Metrics.newTimer(WALBackedPersistenceEngine.class, "put-latencies");
    
    private MemoryPersistenceEngine inMemoryStore;
    private WriteAheadLogger writeAheadLogger;

    public WALBackedPersistenceEngine(String dbFilename) {
        inMemoryStore = new MemoryPersistenceEngine();
        writeAheadLogger = new WriteAheadLogger(dbFilename);
    }
    
    @Override
    public void open() throws IOException {
        inMemoryStore.open();
        writeAheadLogger.open();
    }

    @Override
    public void close() throws IOException {
        inMemoryStore.close();
        writeAheadLogger.close();
    }

    @Override
    public void force_put(String key, DataItem value) throws TException {
        TimerContext context = putLatencyTimer.time();
        try {
            LogEntry logEntry = writeAheadLogger.startLogPut(key, value);
            inMemoryStore.force_put(key, value);
            logEntry.waitUntilPersisted();
        } finally {
            context.stop();
        }
    }

    @Override
    public void put_if_newer(String key, DataItem value) throws TException {
        TimerContext context = putLatencyTimer.time();
        try {
            LogEntry logEntry = writeAheadLogger.startLogPut(key, value);
            inMemoryStore.put_if_newer(key, value);
            logEntry.waitUntilPersisted();
        } finally {
            context.stop();
        }
    }

    @Override
    public DataItem get(String key) throws TException {
        return inMemoryStore.get(key);
    }

    @Override
    public void delete(String key) throws TException {
        inMemoryStore.delete(key); // TODO: should probably write to the WAL here...
    }
}
