package edu.berkeley.thebes.twopl.client;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map.Entry;

import javax.naming.ConfigurationException;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;

/** Client buffers a transaction and sends it off at the END.
 * Accordingly, GET and PUT cannot return valid values. */
public class ThebesTwoPLClient implements IThebesClient {
    private boolean inTransaction;
    
    private List<String> xactCommands;
    private TwoPLTransactionService.Client xactClient;

    
    @Override
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException {
        InetSocketAddress addr = Config.getTwoPLTransactionManagerBindIP();
        xactClient = TwoPLThriftUtil.getTransactionServiceClient(
                addr.getHostString(), addr.getPort());
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
        return true;
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        if (!inTransaction) {
            throw new TException("Must be in a transaction!");
        }
        xactCommands.add("get " + key);
        return null;
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