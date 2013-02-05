package edu.berkeley.thebes.hat.client;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.clustering.ReplicaRouter;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

public class ThebesHATClient implements IThebesClient {
    private ReplicaRouter router;
    private Map<String, DataItem> transactionWriteBuffer;
    private Map<String, DataItem> transactionReadBuffer;
    private Config.IsolationLevel isolationLevel = Config.getThebesIsolationLevel();
    private boolean transactionInProgress = false;

    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        router = new ReplicaRouter();
    }

    @Override
    public void beginTransaction() throws TException {
        if(isolationLevel.ordinal() >= Config.IsolationLevel.READ_COMMITTED.ordinal()) {
            transactionWriteBuffer = new HashMap<String, DataItem>();
        }
        if(isolationLevel.ordinal() >= Config.IsolationLevel.REPEATABLE_READ.ordinal()) {
            transactionReadBuffer = new HashMap<String, DataItem>();
        }

        transactionInProgress = true;
    }

    @Override
    public boolean endTransaction() throws TException {
        transactionInProgress = false;

        if(isolationLevel.ordinal() > Config.IsolationLevel.NO_ISOLATION.ordinal()) {
            for(String key : transactionWriteBuffer.keySet()) {
                router.getReplicaByKey(key).put(key, transactionWriteBuffer.get(key));
            }
        }

        return true;
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        if(!transactionInProgress)
            throw new TException("transaction is not in progress");

        long timestamp = System.currentTimeMillis();
        DataItem dataItem = new DataItem(value, timestamp);

        if(isolationLevel.ordinal() > Config.IsolationLevel.NO_ISOLATION.ordinal()) {
            if(isolationLevel == Config.IsolationLevel.REPEATABLE_READ) {
                transactionReadBuffer.put(key, dataItem);
            }

            transactionWriteBuffer.put(key, dataItem);
            return true;
        }
        else {
            return router.getReplicaByKey(key).put(key, dataItem);
        }
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        if(!transactionInProgress)
            throw new TException("transaction is not in progress");

        if(isolationLevel == Config.IsolationLevel.REPEATABLE_READ && transactionReadBuffer.containsKey(key)) {
            return transactionReadBuffer.get(key).data;
        }

        DataItem ret = router.getReplicaByKey(key).get(key);

        if(isolationLevel == Config.IsolationLevel.REPEATABLE_READ) {
            transactionReadBuffer.put(key, ret);
        }

        return ret.data;
    }
    
    @Override
    public void sendCommand(String cmd) throws TException {
        throw new UnsupportedOperationException();
    }

    public void close() { return; }
}