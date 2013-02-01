package edu.berkeley.thebes.twopl.client;

import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService.Client;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Sets;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.Set;

public class ThebesTwoPLClient implements IThebesClient {
    private Random sessionIdGen = new Random();
    private long sessionId;
    private boolean inTransaction;
    private Set<String> lockedKeys;
    private TwoPLMasterRouter masterRouter;
    
    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        masterRouter = new TwoPLMasterRouter();
    }

    @Override
    public void beginTransaction() throws TException {
        if (inTransaction) {
            throw new TException("Currently in a transaction.");
        }
        sessionId = sessionIdGen.nextLong();
        inTransaction = true;
        lockedKeys = Sets.newHashSet();
    }

    @Override
    public boolean endTransaction() throws TException {
        for (String key : lockedKeys) {
            masterRouter.getMasterByKey(key).unlock(sessionId, key);
        }
        inTransaction = false;
        return true;
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        
        long timestamp = System.currentTimeMillis();
        DataItem dataItem = new DataItem(value, timestamp);
        acquireLock(key);
        return masterRouter.getMasterByKey(key).put(sessionId, key, dataItem);
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        
        acquireLock(key);
        DataItem dataItem = masterRouter.getMasterByKey(key).get(sessionId, key);
        return ByteBuffer.wrap(dataItem.getData());
    }
    
    /** Ensures we own the given lock. If not, we acquire it or die trying. */
    private void acquireLock(String key) throws TException {
        Client master = masterRouter.getMasterByKey(key);
        if (!lockedKeys.contains(key)) {
            boolean lockAcquired = master.lock(sessionId, key);
            if (!lockAcquired) {
                throw new TException("Lock could not be acquired for key '" + key + "'");
            } else {
                lockedKeys.add(key);
            }
        }
    }

    public void close() { return; }
}