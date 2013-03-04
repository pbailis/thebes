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

import edu.berkeley.thebes.common.thrift.ServerAddress;
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

        public void clear() {
            versions.clear();
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
    private Map<String, DataItem> transactionWriteBuffer;
    private Map<String, DataItem> transactionReadBuffer;

    //Session guarantee data structures
    private SessionLevel sessionLevel = Config.getThebesSessionLevel();

    //Atomicity data structures
    private AtomicityLevel atomicityLevel = Config.getThebesAtomicityLevel();
    private VersionVector atomicityVersionVector;

    public ThebesHATClient() {
        if(atomicityLevel != AtomicityLevel.NO_ATOMICITY &&
           !isolationLevel.atOrHigher(IsolationLevel.READ_COMMITTED)) {
            /*
              Begs the question: why have TA and RC as separate? Answer is that this may change if we go with the
              more permissive "broad interpretation" of RC: c.f., P1 vs. A1 in Berensen et al., SIGMOD '95
             */
            throw new IllegalStateException("Transactional atomicity guarantees must run at isolation of RC or higher");
        }
    }

    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        router = new ReplicaRouter();
        atomicityVersionVector = new VersionVector();
    }

    @Override
    public void beginTransaction() throws TException {
        transactionWriteBuffer = Maps.newHashMap();
        transactionReadBuffer = Maps.newHashMap();
        atomicityVersionVector.clear();
        transactionInProgress = true;
    }

    private void applyWritesInBuffer() {
        final Semaphore parallelWriteSemaphore = new Semaphore(transactionWriteBuffer.size());
        final Version transactionVersion = new Version(clientID, System.currentTimeMillis());
        final List<String> transactionKeys = new ArrayList<String>(transactionWriteBuffer.keySet())
                                                                                                  ;
        //todo: client is not threadsafe!
        for(final String key : transactionWriteBuffer.keySet()) {
            new Thread(new Runnable() {
                @Override
                public void run() {
                    DataItem queuedWrite = transactionWriteBuffer.get(key);
                    try {
                        if(atomicityLevel != AtomicityLevel.CLIENT) {
                            queuedWrite.setVersion(transactionVersion);
                            queuedWrite.setTransactionKeys(transactionKeys);
                        }

                        doPut(key,
                              queuedWrite,
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

        if(atomicityLevel != AtomicityLevel.CLIENT)
            atomicityVersionVector.updateVector(new ArrayList<String>(transactionWriteBuffer.keySet()),
                                                transactionVersion);
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
            transactionWriteBuffer.put(key, dataItem);

            return true;
        }
        else {
            return doPut(key, dataItem, new ArrayList<String>());
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
            if(ret != null) {
                /*
                  If the value we just read was part of a transaction that i.) wrote to a key
                  we've already read and ii.) is ordered after the transaction that wrote to
                  that key, then we can't show it. For now, return null.
                */
                for(String atomicKey : ret.getTransactionKeys()) {
                    if(atomicityVersionVector.getVersion(atomicKey) != null &&
                       atomicityVersionVector.getVersion(atomicKey).compareTo(ret.getVersion()) < 0) {
                        transactionReadBuffer.put(atomicKey, new DataItem(null, Version.NULL_VERSION));
                        return null;
                    }
                }

                atomicityVersionVector.updateVector(ret.getTransactionKeys(), ret.getVersion());
                transactionReadBuffer.put(key, ret);
            }
            else {
                /*
                  If we read a null, we should read null for future reads too!
                 */
                transactionReadBuffer.put(key, new DataItem(null, Version.NULL_VERSION));
                return null;
            }
        }

        // if this branch evaluates to true, then we're using Transactional Atomicity or RC or greater
        if(transactionWriteBuffer.containsKey(key)) {
            if (transactionWriteBuffer.get(key).getVersion()
            		.compareTo(ret.getVersion()) > 0) {
                return transactionWriteBuffer.get(key).getData();
            }
        }

        if(atomicityLevel != AtomicityLevel.NO_ATOMICITY && ret != null && ret.getTransactionKeys() != null) {
            atomicityVersionVector.updateVector(ret.getTransactionKeys(), ret.getVersion());
        }

        return ret.getData();
    }


    private boolean doPut(String key,
                          DataItem value,
                          Set<String> transactionKeys) throws TException {
            return doPut(key, value,  new ArrayList<String>(transactionKeys));
    }

    /*
        Needs to be threadsafe
     */

    private boolean doPut(String key,
                          DataItem value,
                          List<String> transactionKeys) throws TException {
        TimerContext timer = latencyPerOperationMetric.time();
        boolean ret;

        try {
            ret = router.getReplicaByKey(key).put(key,
                                                  DataItem.toThrift(value),
                                                  transactionKeys);
        } catch (RuntimeException e) {
            errorMetric.mark();
            throw e;
        } catch (TException e) {
            errorMetric.mark();
            throw new TException("exception on replica "+router.getReplicaIPByKey(key)+" "+e.getMessage());
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
            throw new TException("exception on replica "+router.getReplicaIPByKey(key)+" "+e.getMessage());
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