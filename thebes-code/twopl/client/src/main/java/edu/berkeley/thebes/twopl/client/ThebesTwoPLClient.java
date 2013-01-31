package edu.berkeley.thebes.twopl.client;

import edu.berkeley.thebes.common.interfaces.IThebesClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

public class ThebesTwoPLClient implements IThebesClient {
    @Override
    public void open(String [] args) throws TTransportException, ConfigurationException, FileNotFoundException {
    }

    @Override
    public void beginTransaction() throws TTransportException {}

    @Override
    public boolean endTransaction() throws TTransportException { return true; }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        return true;
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        return ByteBuffer.wrap("".getBytes());
    }

    public void close() { return; }
}