package edu.berkeley.thebes.hat.client;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.ConfigurationException;

import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
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
import edu.berkeley.thebes.common.log4j.Log4JConfig;
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
    
    private static final AtomicInteger LOGICAL_CLOCK = new AtomicInteger(0);

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

    private class TransactionMultiPutCallback implements AsyncMethodCallback<ReplicaService.AsyncClient.put_call> {
        private Semaphore blockFor;
        private final int numWrites;
        private List<Exception> exceptionList = Lists.newArrayList();

        public TransactionMultiPutCallback(int numWrites) {
            blockFor = new Semaphore(numWrites);
            this.numWrites = numWrites;
        }

        @Override
        public void onComplete(ReplicaService.AsyncClient.put_call put_call) {
            blockFor.release();
        }

        @Override
        public void onError(Exception e) {
            exceptionList.add(e);
            blockFor.release();
        }

        public void blockForWrites() throws TException {
            try {
                blockFor.acquire(numWrites);
            } catch (InterruptedException e) {
                exceptionList.add(e);
            } finally {
                if(!exceptionList.isEmpty()) {
                    String exceptionString = "Exceptions occured in processing write: ";
                    for(Exception e : exceptionList) {
                        exceptionString += e.getMessage() + e.getStackTrace();
                    }

                    throw new TException(exceptionString);
                }
            }
        }
    }

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
    public void open() throws TTransportException, ConfigurationException, IOException {
        router = ReplicaRouter.newInstance(Config.getRoutingMode());
        atomicityVersionVector = new VersionVector();
    }

    @Override
    public void beginTransaction() throws TException {
        transactionWriteBuffer = Maps.newHashMap();
        transactionReadBuffer = Maps.newHashMap();
        atomicityVersionVector.clear();
        transactionInProgress = true;
    }

    private void applyWritesInBuffer() throws TException {
        Version transactionVersion = new Version(clientID, LOGICAL_CLOCK.incrementAndGet(),
                System.currentTimeMillis());
        List<String> transactionKeys = new ArrayList<String>(transactionWriteBuffer.keySet());

        //TransactionMultiPutCallback callback = new TransactionMultiPutCallback(transactionKeys.size());
        for(String key : transactionKeys) {
            DataItem queuedWrite = transactionWriteBuffer.get(key);

            List<String> transactionalDependencies = null;

            if(isolationLevel.atOrHigher(IsolationLevel.READ_COMMITTED)) {
                queuedWrite.setVersion(transactionVersion);
            }
            if(atomicityLevel == AtomicityLevel.CLIENT) {
                queuedWrite.setTransactionKeys(transactionKeys);
            }

            doPutSync(key,
                      queuedWrite);

            /*
            doPutAsync(key,
                       queuedWrite,
                       transactionKeys,
                       callback);
                       */

        }

        //callback.blockForWrites();

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
        DataItem dataItem = new DataItem(value,
                new Version(clientID, LOGICAL_CLOCK.incrementAndGet(), timestamp));

        if(isolationLevel.higherThan(IsolationLevel.NO_ISOLATION) || atomicityLevel != AtomicityLevel.NO_ATOMICITY) {
            if(isolationLevel == IsolationLevel.REPEATABLE_READ) {
                transactionReadBuffer.put(key, dataItem);
            }
            transactionWriteBuffer.put(key, dataItem);

            return true;
        }
        else {
            return doPutSync(key, dataItem);
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
            if(ret != null && ret.getTransactionKeys() != null) {
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
            if (ret == null || transactionWriteBuffer.get(key).getVersion()
            		           .compareTo(ret.getVersion()) > 0) {
                return transactionWriteBuffer.get(key).getData();
            }
        }

        if(atomicityLevel != AtomicityLevel.NO_ATOMICITY && ret != null && ret.getTransactionKeys() != null) {
            atomicityVersionVector.updateVector(ret.getTransactionKeys(), ret.getVersion());
        }

        return ret == null ? null : ret.getData();
    }

    private boolean doPutSync(String key,
                              DataItem value) throws TException {
        TimerContext timer = latencyPerOperationMetric.time();
        boolean ret;

        try {
            ret = router.put(key, value);
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

//    private boolean doPutAsync(String key,
//                               DataItem value,
//                               TransactionMultiPutCallback callback) throws TException {
//        TimerContext timer = latencyPerOperationMetric.time();
//
//        try {
//            router.put(key,
//                                                 value.toThrift(),
//                                                 callback);
//        } catch (RuntimeException e) {
//            errorMetric.mark();
//            throw e;
//        } catch (TException e) {
//            errorMetric.mark();
//            throw new TException("exception on replica "+router.getReplicaIPByKey(key)+" "+e.getMessage());
//        } finally {
//            timer.stop();
//        }
//        return true;
//    }
    
    private DataItem doGet(String key) throws TException {
        TimerContext timer = latencyPerOperationMetric.time();
        DataItem ret;
        try {
            ThriftDataItem tdrRet = router.get(key, atomicityVersionVector.getVersion(key));
            if(tdrRet.getData() == null)
                return null;

            ret = new DataItem(tdrRet);
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