package com.yahoo.ycsb.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteArrayByteIterator;
import org.apache.log4j.Logger;

import edu.berkeley.thebes.client.ThebesClient;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;


public class ThebesYCSBClient extends DB {

    ThebesClient client;

    private final Logger logger = Logger.getLogger(ThebesYCSBClient.class);
    
    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;
    
	public void init() throws DBException {
        client = new ThebesClient();
        try {
            client.open();
        } catch (Exception e) {
            throw new DBException(e.getMessage());
        }
	}
	
	public void cleanup() throws DBException {
        client.close();
	}

    public int beginTransaction() {
        try {
            client.beginTransaction();
        } catch (Exception e) {
            logger.warn(e.getMessage());
            return ERROR;
        }

        return OK;
    }

    public int endTransaction() {
        try {
            client.endTransaction();
        } catch (Exception e) {
            logger.warn(e.getMessage());
            return ERROR;
        }

        return OK;
    }
	
	@Override
	public int delete(String table, String key) {
        try {
            client.put(key, null);
        } catch (Exception e) {
            logger.warn(e.getMessage());
            return ERROR;
        }
        return OK;
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        try {
            client.put(key, ByteBuffer.wrap(values.values().iterator().next().toArray()));
        } catch (Exception e) {
            logger.warn(e.getMessage());
            return ERROR;
        }
		return OK;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
        try {
            result.put(fields.iterator().next(), new ByteArrayByteIterator(client.get(key).array()));
        } catch (Exception e) {
            logger.warn(e.getMessage());
            return ERROR;
        }
		return OK;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.warn("Thebes scans are not implemented!");
		return ERROR;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
        //update doesn't pass in the entire record, so we'd need to do read-modify-write
        throw new UnsupportedOperationException("Thebes updates are not implemented!");
	}
	
	private int checkStore(String table) {
		return OK;
	}
}
