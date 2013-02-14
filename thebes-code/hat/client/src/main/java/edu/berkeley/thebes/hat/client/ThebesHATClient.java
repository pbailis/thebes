package edu.berkeley.thebes.hat.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.IsolationLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.SessionLevel;

import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.client.clustering.ReplicaRouter;

import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ThebesHATClient implements IThebesClient {

    private class QueuedWrite {
        private DataItem write;

        private List<DataDependency> dependencies;

        private QueuedWrite(DataItem write, List<DataDependency> dependencies) {
            this.write = write;
            this.dependencies = Lists.newArrayList(dependencies);
        }

        public List<DataDependency> getDependencies() {
            return dependencies;
        }

        public DataItem getWrite() {
            return write;
        }
    }

    private final Meter requestMetric = Metrics.newMeter(ThebesHATClient.class, "hat-requests", "requests", TimeUnit.SECONDS);
    private final Meter operationMetric = Metrics.newMeter(ThebesHATClient.class, "hat-operations", "operations", TimeUnit.SECONDS);
    private final Meter errorMetric = Metrics.newMeter(ThebesHATClient.class, "hat-errors", "errors", TimeUnit.SECONDS);
    private final Timer latencyBufferedXactMetric = Metrics.newTimer(ThebesHATClient.class, "hat-latencies-buf-xact",
                                                                     TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Timer latencyPerOperationMetric = Metrics.newTimer(ThebesHATClient.class, "hat-latencies-per-op", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    private ReplicaRouter router;

    private boolean transactionInProgress = false;

    //ANSI client-side data structures
    private IsolationLevel isolationLevel = Config.getThebesIsolationLevel();
    private Map<String, QueuedWrite> transactionWriteBuffer;
    private Map<String, DataItem> transactionReadBuffer;

    //Session guarantee data structures
    private SessionLevel sessionLevel = Config.getThebesSessionLevel();
    private List<DataDependency> causalDependencies = null;

    private void rebaseCausalDependencies(String key, DataItem value) {
        if(sessionLevel == SessionLevel.CAUSAL) {
            causalDependencies.clear();
            causalDependencies.add(new DataDependency(key, value.getTimestamp()));
        }
    }

    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        router = new ReplicaRouter();
        causalDependencies = new ArrayList<DataDependency>();
    }

    @Override
    public void beginTransaction() throws TException {
        if(isolationLevel.atOrHigher(IsolationLevel.READ_COMMITTED)) {
            transactionWriteBuffer = Maps.newHashMap();
        }
        if(isolationLevel.atOrHigher(IsolationLevel.REPEATABLE_READ)) {
            transactionReadBuffer = Maps.newHashMap();
        }

        transactionInProgress = true;
    }

    @Override
    public boolean endTransaction() throws TException {
        transactionInProgress = false;
        
        requestMetric.mark();

        if(isolationLevel.higherThan(IsolationLevel.NO_ISOLATION)) {
            TimerContext timer = latencyBufferedXactMetric.time();
            try {
                for(String key : transactionWriteBuffer.keySet()) {
                    QueuedWrite queuedWrite = transactionWriteBuffer.get(key);
                    doPut(key, queuedWrite.getWrite(), queuedWrite.getDependencies());
                }
            } finally {
                timer.stop();
            }
        }

        return true;
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        if(!transactionInProgress)
            throw new TException("transaction is not in progress");
        
        operationMetric.mark();

        long timestamp = System.currentTimeMillis();
        DataItem dataItem = new DataItem(value, timestamp);

        if(isolationLevel.higherThan(IsolationLevel.NO_ISOLATION)) {
            if(isolationLevel == IsolationLevel.REPEATABLE_READ) {
                transactionReadBuffer.put(key, dataItem);
            }

            if(sessionLevel != SessionLevel.CAUSAL) {
                transactionWriteBuffer.put(key, new QueuedWrite(dataItem, null));
            }
            else {
                transactionWriteBuffer.put(key, new QueuedWrite(dataItem, causalDependencies));
                rebaseCausalDependencies(key, dataItem);
            }
            return true;
        }
        else {
            boolean ret = doPut(key, dataItem, causalDependencies);
            rebaseCausalDependencies(key, dataItem);
            return ret;
        }
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        if(!transactionInProgress)
            throw new TException("transaction is not in progress");
        
        operationMetric.mark();

        if(isolationLevel == IsolationLevel.REPEATABLE_READ &&
                transactionReadBuffer.containsKey(key)) {
            return transactionReadBuffer.get(key).data;
        }

        DataItem ret = doGet(key);

        if(sessionLevel == SessionLevel.CAUSAL) {
            causalDependencies.add(new DataDependency(key, ret.getTimestamp()));
        }

        if(isolationLevel == IsolationLevel.REPEATABLE_READ) {
            transactionReadBuffer.put(key, ret);
        }

        return ret.data;
    }
    
    private boolean doPut(String key, DataItem value, List<DataDependency> dependencies) throws TException {
        TimerContext timer = latencyPerOperationMetric.time();
        boolean ret;
        try {
            ret = router.getReplicaByKey(key).put(key, value, dependencies);
        } catch (RuntimeException e) {
            errorMetric.mark();
            throw e;
        } catch (TException e) {
            errorMetric.mark();
            throw e;
        } finally {
            timer.stop();
        }
        return ret;
    }
    
    private DataItem doGet(String key) throws TException {
        TimerContext timer = latencyPerOperationMetric.time();
        DataItem ret;
        try {
            ret = router.getReplicaByKey(key).get(key);
        } catch (RuntimeException e) {
            errorMetric.mark();
            throw e;
        } catch (TException e) {
            errorMetric.mark();
            throw e;
        } finally {
            timer.stop();
        }
        return ret;
    }
    
    @Override
    public void sendCommand(String cmd) throws TException {
        throw new UnsupportedOperationException();
    }

    public void close() { return; }
}