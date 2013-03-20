package edu.berkeley.thebes.common.persistence.disk;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;

public class LevelDBPersistenceEngine implements IPersistenceEngine {
    DB db;

    TSerializer serializer = new TSerializer();
    TDeserializer deserializer = new TDeserializer();

    public void open() throws IOException {
        Options options = new Options();
        options.createIfMissing(true);

        if(Config.doCleanDatabaseFile()) {
            try {
                //not proud, but damn Guava for removing removeRecursively
                Runtime.getRuntime().exec("rm -rf "+Config.getDiskDatabaseFile());
            } catch(Exception e) {}
        }


        db = factory.open(new File(Config.getDiskDatabaseFile()), options);
    }

    public boolean put(String key, DataItem value) throws TException {
        db.put(key.getBytes(), serializer.serialize(DataItem.toThrift(value)));
        return true;
    }

    public DataItem get(String key) throws TException {
        byte[] byteRet = db.get(key.getBytes());

        if(byteRet == null)
            return null;

        ThriftDataItem tdrRet = new ThriftDataItem();
        deserializer.deserialize(tdrRet, byteRet);
        return DataItem.fromThrift(tdrRet);
    }

    public void close() throws IOException {
        db.close();
    }
}