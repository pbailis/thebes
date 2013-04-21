package edu.berkeley.thebes.common.persistence.disk;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Uninterruptibles;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import edu.berkeley.thebes.common.data.DataItem;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class WriteAheadLogger {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(WriteAheadLogger.class);

    private final Timer batchPutLatency = Metrics.newTimer(WriteAheadLogger.class, "batch-put-latencies");
    private final Histogram batchSize = Metrics.newHistogram(WriteAheadLogger.class, "batch-size");

    
    private final String dbFilename;
    private final LinkedBlockingQueue<LogEntry> pendingLogEntryQueue;
    private final TSerializer serializer;
    private final ReentrantLock latch = new ReentrantLock();
    private PrintWriter dbStream;
    
    public static class LogEntry {
        private final String key;
        private final ReentrantLock latch; 
        private final String serializedValue;
        private final Condition writeCompleteCondition;
        private final AtomicBoolean writeCompleted;
        
        public LogEntry(String key, String serializedValue, ReentrantLock latch) {
            this.key = key;
            this.serializedValue = serializedValue;
            this.latch = latch;
            this.writeCompleteCondition = latch.newCondition();
            this.writeCompleted = new AtomicBoolean(false);
        }
        
        public void writeCompleted() {
            assert latch.isHeldByCurrentThread();
            this.writeCompleted.set(true);
            this.writeCompleteCondition.signal();
        }
        
        public void waitUntilPersisted() {
            latch.lock();
            try {
                while (!writeCompleted.get()) {
                    writeCompleteCondition.awaitUninterruptibly();
                }
            } finally {
                latch.unlock();
            }
        }
        
        public String toLogLine() {
            return new StringBuilder()
                .append(key.length()).append(".").append(key)
                .append(serializedValue)
                .toString();
        }
    }
    
    public WriteAheadLogger(String dbFilename) {
        this.dbFilename = dbFilename;
        this.pendingLogEntryQueue = new LinkedBlockingQueue<LogEntry>();
        this.serializer = new TSerializer();
    }

    public void open() throws IOException {
        dbStream = new PrintWriter(new FileOutputStream(new File(dbFilename), true /* append */));
        
        new Thread() {
            @Override
            public void run() {
                while (true) {
                    writeWaitingEntries();
                }
            }
        }.start();
    }
    
    public void writeWaitingEntries() {
        TimerContext context = batchPutLatency.time();
        
        try {
            List<LogEntry> logEntries = Lists.newArrayList(); 
            logEntries.add(Uninterruptibles.takeUninterruptibly(pendingLogEntryQueue));
            pendingLogEntryQueue.drainTo(logEntries);
            batchSize.update(logEntries.size());
            
            // Actually store them on disk.
            for (LogEntry logEntry : logEntries) {
                dbStream.println(logEntry.toLogLine());
                dbStream.flush();
            }
    
            // Notify waiting threads.
            latch.lock();
            try {
                for (LogEntry logEntry : logEntries) {
                    logEntry.writeCompleted();
                }
            } finally {
                latch.unlock();
            }
        } finally {
            context.stop();
        }
    }

    /** Enqueues a put to be logged to disk. See {@link LogEntry#waitUntilPersisted()}.*/
    public LogEntry startLogPut(String key, DataItem value) throws TException {
        String serializedValue = new String(serializer.serialize(value.toThrift()));
        LogEntry logEntry = new LogEntry(key, serializedValue, latch);
        pendingLogEntryQueue.add(logEntry);
        return logEntry;
    }

    public void close() throws IOException {
        dbStream.close();
    }
}
