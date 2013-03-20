package edu.berkeley.thebes.common.persistence.disk;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import org.iq80.leveldb.*;
import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;

public class LevelDBPersistenceEngine implements IPersistenceEngine {
    DB db;

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

    public boolean put(String key, DataItem value) {
        db.put(key.getBytes(), DataItem.toThrift(value).getData());
        return true;
    }

    public DataItem get(String key) {
        byte[] byteRet = db.get(key.getBytes());

        if(byteRet == null)
            return null;

        ThriftDataItem tdrRet = new ThriftDataItem();
        return DataItem.fromThrift(tdrRet.setData(byteRet));
    }

    public void close() throws IOException {
        db.close();
    }
}