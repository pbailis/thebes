package edu.berkeley.thebes.hat.client;

import edu.berkeley.thebes.common.clustering.ReplicaRouter;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.interfaces.IThebesClient;
import edu.berkeley.thebes.common.thrift.DataItem;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

public class ThebesHATClient implements IThebesClient {
    private ReplicaRouter router;

    @Override
    public void open(String [] args) throws TTransportException, ConfigurationException, FileNotFoundException {
        router = new ReplicaRouter();
    }

    @Override
    public void beginTransaction() throws TTransportException {}

    @Override
    public boolean endTransaction() throws TTransportException { return true; }

    @Override
    public boolean put(String key, ByteBuffer value) throws TException {
        long timestamp = System.currentTimeMillis();
        DataItem dataItem = new DataItem(value, timestamp); 
        return router.getReplicaByKey(key).put(key, dataItem);
    }

    @Override
    public ByteBuffer get(String key) throws TException {
        return ByteBuffer.wrap(router.getReplicaByKey(key).get(key).getData());
    }

    public void close() { return; }
}