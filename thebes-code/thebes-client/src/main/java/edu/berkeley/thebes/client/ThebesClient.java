package edu.berkeley.thebes.client;

import edu.berkeley.thebes.common.clustering.ReplicaRouter;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigStrings;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.hat.client.ThebesHATClient;
import edu.berkeley.thebes.twopl.client.ThebesTwoPLClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

// Wrapper class for various thebes clients

public class ThebesClient implements IThebesClient {
    private IThebesClient internalClient;

    public ThebesClient() {
    }

    @Override
    public void open(String [] args) throws TTransportException, ConfigurationException, FileNotFoundException {
        Config.initializeClientConfig(args);
        if(Config.getThebesTxnMode().equals(ConfigStrings.HAT_MODE)) {
            internalClient = new ThebesHATClient();
        }
        else if(Config.getThebesTxnMode().equals(ConfigStrings.TWOPL_MODE)) {
            internalClient = new ThebesTwoPLClient();
        }
        else {
            throw new ConfigurationException(String.format("invalid transaction mode: %s", Config.getThebesTxnMode()));
        }

        internalClient.open(args);
    }

    @Override
    public void beginTransaction() throws TTransportException {
        internalClient.beginTransaction();
    }

    @Override
    public boolean endTransaction() throws TTransportException {
        return internalClient.endTransaction();
    }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        return internalClient.put(key, value);
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        return internalClient.get(key);
    }

    public void close() {
        internalClient.close();
    }
}