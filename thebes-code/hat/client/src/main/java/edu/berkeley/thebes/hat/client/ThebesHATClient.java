package edu.berkeley.thebes.hat.client;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.TimerContext;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.AtomicityLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.IsolationLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.SessionLevel;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.hat.client.clustering.ReplicaRouter;
import edu.berkeley.thebes.hat.common.data.DataDependency;

public class ThebesHATClient implements IThebesClient {
    private static Logger logger = LoggerFactory.getLogger(ThebesHATClient.class);

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

    private class VersionVector {
        private Map<String, Version> versions = Maps.newHashMap();

        public void updateVector(List<String> keys, Version newVersion) {
            for (String key : keys) {
                if (!versions.containsKey(key) || newVersion.compareTo(versions.get(key)) > 0) {
                    versions.put(key, newVersion);
                }
            }
        }

        public Version getVersion(String key) {
            if(versions.containsKey(key))
                return versions.get(key);

            return null;
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

    private final short clientID = Config.getClientID();

    //ANSI client-side data structures
    private IsolationLevel isolationLevel = Config.getThebesIsolationLevel();
    private Map<String, QueuedWrite> transactionWriteBuffer;
    private Map<String, DataItem> transactionReadBuffer;

    //Session guarantee data structures
    private SessionLevel sessionLevel = Config.getThebesSessionLevel();
    private List<DataDependency> causalDependencies;


    //Atomicity data structures
    private AtomicityLevel atomicityLevel = Config.getThebesAtomicityLevel();
    private VersionVector atomicityVersionVector;

    private void addCausalDependency(String key, DataItem value) {
        /*
         We don't want to have a dependency both in causalDependencies and in the
         transactional atomicity. Transactional atomicity is a strongly connected component,
         causalDependencies form a DAG.
         */
        if(atomicityLevel == AtomicityLevel.NO_ATOMICITY || clientID != value.getVersion().getClientID())
            causalDependencies.add(new DataDependency(key, value.getVersion()));
    }

    private void rebaseCausalDependencies(String key, DataItem value) {
        if(sessionLevel == SessionLevel.CAUSAL) {
            causalDependencies.clear();

            addCausalDependency(key, value);
        }
    }

    public ThebesHATClient() {
        if(isolationLevel.atOrHigher(IsolationLevel.READ_COMMITTED) &&
           sessionLevel == SessionLevel.CAUSAL &&
           atomicityLevel == AtomicityLevel.NO_ATOMICITY) {
            /*
              Begs the question: why have TA and RC as separate? Answer is that this may change if we go with the
              more permissive "broad interpretation" of RC: c.f., P1 vs. A1 in Berensen et al., SIGMOD '95
             */
            throw new IllegalStateException("Isolation of RC or higher with causality guarantees must" +
                                            "be accompanied by transactional atomicity at this time.");
        }
    }

    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        router = new ReplicaRouter();
        causalDependencies = Lists.newArrayList();
        atomicityVersionVector = new VersionVector();
    }

    @Override
    public void beginTransaction() throws TException {
        if(isolationLevel.atOrHigher(IsolationLevel.READ_COMMITTED) || atomicityLevel != AtomicityLevel.NO_ATOMICITY) {
            transactionWriteBuffer = Maps.newHashMap();
        }
        if(isolationLevel.atOrHigher(IsolationLevel.REPEATABLE_READ)) {
            transactionReadBuffer = Maps.newHashMap();
        }

        transactionInProgress = true;
    }

    private void applyWritesInBuffer() {
        final Semaphore parallelWriteSemaphore = new Semaphore(transactionWriteBuffer.size());
        for(final String key : transactionWriteBuffer.keySet()) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    QueuedWrite queuedWrite = transactionWriteBuffer.get(key);
                    try {
                        doPut(key,
                              queuedWrite.getWrite(),
                              queuedWrite.getDependencies(),
                              transactionWriteBuffer.keySet());
                    } catch (TException e) {
                        logger.warn(e.getMessage());
                    } finally {
                        parallelWriteSemaphore.release();
                    }
                }
            }).start();
        }

        try {
            for(int i = 0; i < transactionWriteBuffer.size(); ++i)
                parallelWriteSemaphore.acquire();
        } catch (InterruptedException e) {
            logger.warn(e.getMessage());
        }
    }

    @Override
    public boolean endTransaction() throws TException {
        transactionInProgress = false;
        
        requestMetric.mark();

        if(isolationLevel.higherThan(IsolationLevel.NO_ISOLATION)) {
            TimerContext timer = latencyBufferedXactMetric.time();
            applyWritesInBuffer();
            timer.stop();
        }

        transactionWriteBuffer.clear();
        transactionReadBuffer.clear();

        return true;
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        if(!transactionInProgress)
            throw new TException("transaction is not in progress");
        
        operationMetric.mark();

        long timestamp = System.currentTimeMillis();
        DataItem dataItem = new DataItem(value, new Version(clientID,  timestamp));

        if(isolationLevel.higherThan(IsolationLevel.NO_ISOLATION) || atomicityLevel != AtomicityLevel.NO_ATOMICITY) {
            if(isolationLevel == IsolationLevel.REPEATABLE_READ) {
                transactionReadBuffer.put(key, dataItem);
            }

            transactionWriteBuffer.put(key, new QueuedWrite(dataItem, causalDependencies));
            rebaseCausalDependencies(key, dataItem);

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
            return transactionReadBuffer.get(key).getData();
        }

        DataItem ret = doGet(key);

        if(isolationLevel == IsolationLevel.REPEATABLE_READ) {
            if(ret != null)
                transactionReadBuffer.put(key, ret);
            else
                /*
                  If we read a null, we should read null for future reads too!
                 */
                transactionReadBuffer.put(key, new DataItem(null, Version.NULL_VERSION));
        }

        // if this branch evaluates to true, then we're using Transactional Atomicity or RC or greater
        if(transactionWriteBuffer.containsKey(key)) {
            if (transactionWriteBuffer.get(key).getWrite().getVersion()
            		.compareTo(ret.getVersion()) > 0) {
                return transactionWriteBuffer.get(key).getWrite().getData();
        }

        if(atomicityLevel != AtomicityLevel.NO_ATOMICITY && ret != null && ret.getTransactionKeys() != null) {
            atomicityVersionVector.updateVector(ret.getTransactionKeys(), ret.getVersion());
        }

        // TODO(pbailis) Uhh... cannot resolve...
<<<<<<< HEAD
        if(atomicityLevel != AtomicityLevel.NO_ATOMICITY || isolationLevel.atOrHigher(IsolationLevel.READ_COMMITTED)) {
            if (transactionWriteBuffer.get(key).getWrite().getVersion()
            		.compareTo(ret.getVersion()) > 0) {
                return transactionWriteBuffer.get(key).getWrite().getData();
            }
=======
        if(sessionLevel == SessionLevel.CAUSAL && ret != null) {
            addCausalDependency(key, ret);
>>>>>>> 4726ea7806a4c4898e6a4f5f6ae58b44c8ada782
        }

        return ret.getData();
    }

    private boolean doPut(String key,
                          DataItem value,
                          List<DataDependency> causalDependencies) throws TException {
            return doPut(key, value,  causalDependencies, new ArrayList<String>());
    }

    private boolean doPut(String key,
                          DataItem value,
                          List<DataDependency> causalDependencies,
                          Set<String> transactionKeys) throws TException {
            return doPut(key, value,  causalDependencies, new ArrayList<String>(transactionKeys));
    }

    private boolean doPut(String key,
                          DataItem value,
                          List<DataDependency> causalDependencies,
                          List<String> transactionKeys) throws TException {
        TimerContext timer = latencyPerOperationMetric.time();
        boolean ret;
        try {
            ret = router.getReplicaByKey(key).put(key, DataItem.toThrift(value),
            		DataDependency.toThrift(causalDependencies), transactionKeys);
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
            ret = DataItem.fromThrift(router.getReplicaByKey(key).get(key,
            		Version.toThrift(atomicityVersionVector.getVersion(key))));
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