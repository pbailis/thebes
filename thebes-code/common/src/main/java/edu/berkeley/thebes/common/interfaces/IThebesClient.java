package edu.berkeley.thebes.common.interfaces;

import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

import javax.naming.ConfigurationException;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

public interface IThebesClient {
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException;

    public void beginTransaction() throws TException;
    public boolean endTransaction() throws TException;

    public boolean put(String key, ByteBuffer value) throws TException;
    public ByteBuffer get(String key) throws TException;
    /** Sends an arbitrary, parseable command. */  
    public void sendCommand(String cmd) throws TException;

    public void close();
}