package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.Set;
import java.util.Vector;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;


public class ThebesClient extends DB {

    private final Logger logger = Logger.getLogger(ThebesClient.class);
    
    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;
    
	public void init() throws DBException {
	}
	
	public void cleanup() throws DBException {
	}
	
	@Override
	public int delete(String table, String key) {
            return OK;
	}

	@Override
	public int insert(String table, String key, HashMap<String, ByteIterator> values) {
		return OK;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
		return OK;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		return OK;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
		return OK;
	}
	
	private int checkStore(String table) {
		return OK;
	}

}
