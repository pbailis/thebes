package edu.berkeley.thebes.common.persistence.disk;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;
import com.sleepycat.je.Transaction;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.util.LockManager;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import org.apache.commons.io.FileUtils;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class BDBPersistenceEngine implements IPersistenceEngine {

    private static org.slf4j.Logger logger = LoggerFactory.getLogger(BDBPersistenceEngine.class);
    Database db;
    Environment env;

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
        if(Config.doCleanDatabaseFile()) {
            try {
                FileUtils.forceDelete(new File(Config.getDiskDatabaseFile()));
            } catch(Exception e) {
                if (!(e instanceof FileNotFoundException))
                    logger.warn("error: ", e) ;
            }
        }

        new File(Config.getDiskDatabaseFile()).mkdirs();

        EnvironmentConfig envConfig = new EnvironmentConfig();
        envConfig.setAllowCreate(true);
        envConfig.setTransactional(true);
        env = new Environment(new File(Config.getDiskDatabaseFile()), envConfig);

        DatabaseConfig dbConfig = new DatabaseConfig();
        dbConfig.setAllowCreate(true);
        dbConfig.setTransactional(true);
        db = env.openDatabase(null, "thebesDB", dbConfig);

    }

    public boolean put(String key, DataItem value) throws TException {
        if(value == null) {
            logger.warn("NULL write to key "+key);
            return true;
        }

        Transaction putTxn = env.beginTransaction(null, null);

        try {
            DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
            DatabaseEntry existingEntry = new DatabaseEntry();
            db.get(putTxn, keyEntry, existingEntry, LockMode.RMW);

            if(existingEntry.getData() != null) {
                ThriftDataItem existingThriftItem = new ThriftDataItem();
                deserializer.get().deserialize(existingThriftItem, existingEntry.getData());
                DataItem existingDataItem = new DataItem(existingThriftItem);

                if (existingDataItem.getVersion().compareTo(value.getVersion()) <= 0) {
                    return false;
                }
            }

            DatabaseEntry newDataEntry = new DatabaseEntry(serializer.get().serialize(value.toThrift()));
            db.put(putTxn,  keyEntry, newDataEntry);
        } catch(Exception e) {
            logger.warn("error: ", e);

            if (putTxn != null) {
                putTxn.abort();
                putTxn = null;
            }

            return false;
        } finally {
            if(putTxn != null)
            putTxn.commit();
        }
        return true;
    }

    public DataItem get(String key) throws TException {

        DatabaseEntry keyEntry = new DatabaseEntry(key.getBytes());
        DatabaseEntry dataEntry = new DatabaseEntry();

        OperationStatus status = db.get(null, keyEntry, dataEntry, LockMode.DEFAULT);

        if(status != OperationStatus.SUCCESS || dataEntry.getData() == null)
            return null;

        ThriftDataItem tdrRet = new ThriftDataItem();
        deserializer.get().deserialize(tdrRet, dataEntry.getData());
        return new DataItem(tdrRet);
    }

    public void close() throws IOException {
        db.close();
        env.close();
    }

}