package edu.berkeley.thebes.twopl.client;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.naming.ConfigurationException;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.TwoPLMasterRouter;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;

/** Client buffers a transaction and sends it off at the END.
 * Accordingly, GET and PUT cannot return valid values. */
public class ThebesTwoPLClient implements IThebesClient {
    private boolean inTransaction;
    
    private List<String> xactCommands;
    private TwoPLMasterRouter masterRouter;
    private ConcurrentMap<Integer, AtomicInteger> clusterToAccessesMap;
    private Set<String> observedKeys;
    
    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        InetSocketAddress addr = Config.getTwoPLTransactionManagerBindIP();
        masterRouter = new TwoPLMasterRouter();
        clusterToAccessesMap = Maps.newConcurrentMap();
        observedKeys = Sets.newHashSet();
    }

    @Override
    public void beginTransaction() throws TException {
        if (inTransaction) {
            throw new TException("Currently in a transaction.");
        }
        xactCommands = Lists.newArrayList();
        inTransaction = true;
    }

    @Override
    public boolean endTransaction() throws TException {
        if (!inTransaction) {
            return false;
        }
        
        // Open the transaction client with the TM that's closest to the most-used masters.
        int max = -1;
        Integer maxClusterID = null;
        for (Entry<Integer, AtomicInteger> clusterIDCount : clusterToAccessesMap.entrySet()) {
            if (maxClusterID == null || clusterIDCount.getValue().get() > max) {
                maxClusterID = clusterIDCount.getKey();
                max = clusterIDCount.getValue().get();
            }
        }
        ServerAddress bestTM = Config.getTwoPLTransactionManagerByCluster(maxClusterID); 
        TwoPLTransactionService.Client xactClient =
                TwoPLThriftUtil.getTransactionServiceClient(bestTM.getIP(), bestTM.getPort());
        
        inTransaction = false;
        TwoPLTransactionResult result;
        try {
            result = xactClient.execute(xactCommands);
            System.out.println("Transaction committed successfully.");
            
            for (Entry<String, ByteBuffer> value : result.requestedValues.entrySet()) {
                System.out.println("Returned: " + value.getKey() + " -> "
                        + value.getValue().getInt());
            }
            return true;
        } catch (TTransactionAbortedException e) {
            System.out.println("ERROR: " + e.getErrorMessage());
            System.out.println("Transaction aborted.");
            return false;
        }
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        xactCommands.add("put " + key + " " + new String(value.array()));
        
        incrementCluster(key);
        return true;
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        xactCommands.add("get " + key);
        incrementCluster(key);
        return null;
    }
    
    private void incrementCluster(String key) {
        if (observedKeys.contains(key))
            return;
        observedKeys.add(key);
        
        ServerAddress address = masterRouter.getMasterAddressByKey(key);
        int clusterID = address.getClusterID();
        clusterToAccessesMap.putIfAbsent(clusterID, new AtomicInteger(0));
        clusterToAccessesMap.get(clusterID).incrementAndGet();
    }

    /** Adds a raw command accepted by the Thebes Transactional Language (TTL). */
    @Override
    public void sendCommand(String cmd) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        xactCommands.add(cmd);
    }
    
    public void close() { return; }
}