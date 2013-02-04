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
        TwoPLTransactionResult result = xactClient.execute(xactCommands);
        if (result.success) {
            System.out.println("Transaction committed successfully.");
        } else {
            System.out.println("ERROR: " + result.errorString);
            System.out.println("Transaction aborted.");
        }
        for (Entry<String, ByteBuffer> value : result.requestedValues.entrySet()) {
            System.out.println("Returned: " + value.getKey() + " -> "
                    + value.getValue().getInt());
        }
        return true;
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