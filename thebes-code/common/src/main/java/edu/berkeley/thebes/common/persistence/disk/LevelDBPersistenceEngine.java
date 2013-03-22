package edu.berkeley.thebes.common.persistence.disk;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.util.LockManager;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.iq80.leveldb.*;
import org.slf4j.*;

import static org.fusesource.leveldbjni.JniDBFactory.*;
import java.io.*;

public class LevelDBPersistenceEngine implements IPersistenceEngine {
    DB db;
    LockManager lockManager;
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(LevelDBPersistenceEngine.class);


    ThreadLocal<TSerializer> serializer = new ThreadLocal<TSerializer>() {
        @Override
        protected TSerializer initialValue() {
            return new TSerializer();
        }
    };

    ThreadLocal<TDeserializer> deserializer = new ThreadLocal<TDeserializer>() {
        @Override
        protected TDeserializer initialValue() {
            return new TDeserializer();
        }
    };

    public void open() throws IOException {
        Options options = new Options();
        options.createIfMissing(true);
        lockManager = new LockManager();

        if(Config.doCleanDatabaseFile()) {
            try {
                FileUtils.forceDelete(new File(Config.getDiskDatabaseFile()));
            } catch(Exception e) {
                if (!(e instanceof FileNotFoundException))
                    logger.warn("error: ", e) ;
            }
        }

        db = factory.open(new File(Config.getDiskDatabaseFile()), options);
    }

    public boolean put(String key, DataItem value) throws TException {
        if(value == null) {
            logger.warn("NULL write to key "+key);
            return true;
        }

        lockManager.lock(key);
        try {
            DataItem curItem = get(key);

            if (curItem.getVersion().compareTo(value.getVersion()) <= 0) {
                return false;
            }
            else {
                db.put(key.getBytes(), serializer.get().serialize(value.toThrift()));
                return true;
            }
        } finally {
            lockManager.unlock(key);
        }
    }

    public DataItem get(String key) throws TException {
        byte[] byteRet = db.get(key.getBytes());

        if(byteRet == null)
            return null;

        ThriftDataItem tdrRet = new ThriftDataItem();
        deserializer.get().deserialize(tdrRet, byteRet);
        return new DataItem(tdrRet);
    }

    public void close() throws IOException {
        db.close();
    }
}