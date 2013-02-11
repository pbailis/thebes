package edu.berkeley.thebes.hat.client;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.IsolationLevel;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.clustering.ReplicaRouter;
import edu.berkeley.thebes.twopl.client.ThebesTwoPLClient;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class ThebesHATClient implements IThebesClient {
    private final Meter requestMetric = Metrics.newMeter(ThebesHATClient.class, "hat-requests", "requests", TimeUnit.SECONDS);
    private final Meter operationMetric = Metrics.newMeter(ThebesHATClient.class, "hat-operations", "operations", TimeUnit.SECONDS);
    private final Meter errorMetric = Metrics.newMeter(ThebesTwoPLClient.class, "hat-errors", "errors", TimeUnit.SECONDS);
    private final Timer latencyBufferedXactMetric = Metrics.newTimer(ThebesHATClient.class, "hat-latencies-buf-xact", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);
    private final Timer latencyPerOperationMetric = Metrics.newTimer(ThebesHATClient.class, "hat-latencies-per-op", TimeUnit.MILLISECONDS, TimeUnit.SECONDS);

    
    private ReplicaRouter router;
    private Map<String, DataItem> transactionWriteBuffer;
    private Map<String, DataItem> transactionReadBuffer;
    private IsolationLevel isolationLevel = Config.getThebesIsolationLevel();
    private boolean transactionInProgress = false;

    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        router = new ReplicaRouter();
    }

    @Override
    public void beginTransaction() throws TException {
        if(isolationLevel.atOrHigher(IsolationLevel.READ_COMMITTED)) {
            transactionWriteBuffer = new HashMap<String, DataItem>();
        }
        if(isolationLevel.atOrHigher(IsolationLevel.REPEATABLE_READ)) {
            transactionReadBuffer = new HashMap<String, DataItem>();
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
                    doPut(key, transactionWriteBuffer.get(key));
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

            transactionWriteBuffer.put(key, dataItem);
            return true;
        }
        else {
            return doPut(key, dataItem);
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

        if(isolationLevel == IsolationLevel.REPEATABLE_READ) {
            transactionReadBuffer.put(key, ret);
        }

        return ret.data;
    }
    
    private boolean doPut(String key, DataItem value) throws TException {
        TimerContext timer = latencyPerOperationMetric.time();
        boolean ret;
        try {
            ret = router.getReplicaByKey(key).put(key, value);
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