package edu.berkeley.thebes.twopl.common;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;

import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.interfaces.IThebesClient;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Provides a layer of abstraction that manages getting the actual locks from a set of
 * GET/PUT requests.
 * Communicates directly with the master replicas via a {@link TwoPLMasterRouter}.
 * 
 * Note: This is used by Thebes clients to talk directly with the 2PL servers;
 * alternatively, Thebes clients use {@link ThebesTwoPLClient} to talk to TMS,
 * which use this class to talk to the 2PL servers.
 */
public class ThebesTwoPLTransactionClient implements IThebesClient {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesTwoPLTransactionClient.class);
    
	private static AtomicInteger NEXT_SEQUENCE_NUMBER = new AtomicInteger(0);
    private short clientId = Config.getClientID();
    
    private long sessionId;
    private boolean inTransaction;
    private Set<String> lockedKeys;
    private Set<String> writeLocks;
    private Set<String> readLocks;
    private TwoPLMasterRouter masterRouter;
    
    public ThebesTwoPLTransactionClient() {
    }
    
    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        masterRouter = new TwoPLMasterRouter();
    }

    @Override
    public void beginTransaction() throws TException {
        if (inTransaction) {
            throw new TException("Currently in a transaction.");
        }
        sessionId = Long.parseLong("" + (clientId*100000) + NEXT_SEQUENCE_NUMBER.getAndIncrement());
        inTransaction = true;
        lockedKeys = Sets.newHashSet();
        writeLocks = Sets.newHashSet();
        readLocks = Sets.newHashSet();
    }

    @Override
    public boolean endTransaction() throws TException {
    	inTransaction = false;
    	logger.info("Transaction ended!");
        for (String key : readLocks) {
            masterRouter.getMasterByKey(key).unlock(sessionId, key);
        }
        for (String key : writeLocks) {
            masterRouter.getMasterByKey(key).unlock(sessionId, key);
        }
        return true;
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        
        if (!writeLocks.contains(key)) {
        	if (readLocks.contains(key)) {
        		throw new TException("Cannot upgrade locks right now.");
        	}
        	writeLocks.add(key);
        	try {
        		masterRouter.getMasterByKey(key).write_lock(sessionId, key);
        	} catch (TException e) {
        		e.printStackTrace();
        		throw new TException("Obtaining write lock timed out.");
        	}
        }
        
        long timestamp = System.currentTimeMillis();
        DataItem dataItem = new DataItem(value, new Version(clientId, timestamp));
        return masterRouter.getMasterByKey(key).put(sessionId, key, DataItem.toThrift(dataItem));
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        
        if (!readLocks.contains(key) && !writeLocks.contains(key)) {
        	readLocks.add(key);
        	try {
                masterRouter.getMasterByKey(key).read_lock(sessionId, key);
        	} catch (TException e) {
        		e.printStackTrace();
        		throw new TException("Obtaining write lock timed out.");
        	}
        }
        
        DataItem dataItem = DataItem.fromThrift(masterRouter.getMasterByKey(key).get(sessionId, key));
        // Null is returned by 0-length data
        if (dataItem.getData().limit() == 0) {
            return null;
        }
        return dataItem.getData();
    }
    
    public void writeLock(String key) throws TException {
        lockedKeys.add(key);
        masterRouter.getMasterByKey(key).write_lock(sessionId, key);
    }
    
    public void readLock(String key) throws TException {
        lockedKeys.add(key);
        masterRouter.getMasterByKey(key).read_lock(sessionId, key);
    }

    @Override
    public void sendCommand(String cmd) throws TException {
        throw new UnsupportedOperationException();
    }

    public void close() { return; }
}