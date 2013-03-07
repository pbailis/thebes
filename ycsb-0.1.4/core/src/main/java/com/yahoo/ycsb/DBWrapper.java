/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.yahoo.ycsb;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.measurements.Measurements;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 */
public class DBWrapper extends DB
{
	DB _db;
	TransactionalDB _transactionalDB = null;
	Measurements _measurements;

    //long txStart = -1;

	public DBWrapper(DB db)
	{
		_db=db;
        if(db instanceof TransactionalDB)
            _transactionalDB = (TransactionalDB)db;
		_measurements=Measurements.getMeasurements();
	}

    private void checkTransaction() {

        if(_transactionalDB != null && _transactionalDB.getNextTransactionLength() == requests.size()) {
        	long txStart = System.nanoTime();
        	
//        	Collections.sort(requests);
        	
        	_transactionalDB.beginTransaction();
        	for (Req req : requests) {
        		System.out.println("Req [type=" + req + "] on key=" + req.getKey());
        		if (req instanceof InsertReq) {
        			InsertReq insReq = (InsertReq) req;
	        		long st=System.nanoTime();
	        		int res=_db.insert(insReq.getTable(), insReq.getKey(), insReq.getValues());
//	        		int res=_db.read(req.getTable(), req.getKey(), (Set<String>) null, req.getValues());
	        		long en=System.nanoTime();
	        		System.out.println("INSERT took " + (int)((en-st)/1000));
	        		_measurements.measure("INSERT",(int)((en-st)/1000));
	        		_measurements.reportReturnCode("INSERT",res);
	        		if (res != 0) {
	        			System.err.println("INSERT failed!");
	        			break;
	        		}
        		} else if (req instanceof UpdateReq) {
        			UpdateReq updReq = (UpdateReq) req;
	        		long st=System.nanoTime();
	        		int res=_db.update(updReq.getTable(), updReq.getKey(), updReq.getValues());
//	        		int res=_db.read(req.getTable(), req.getKey(), (Set<String>) null, req.getValues());
	        		long en=System.nanoTime();
	        		System.out.println("UPDATE took " + (int)((en-st)/1000));
	        		_measurements.measure("UPDATE",(int)((en-st)/1000));
	        		_measurements.reportReturnCode("UPDATE",res);
	        		if (res != 0) {
	        			System.err.println("UPDATE failed!");
	        			break;
	        		}
        		} else {
        			GetReq getReq = (GetReq) req;
            		long st=System.nanoTime();
            		int res=_db.read(getReq.getTable(), getReq.getKey(), getReq.getFields(), getReq.getResult());
            		long en=System.nanoTime();
            		System.out.println("READ took " + (int)((en-st)/1000));
            		_measurements.measure("READ",(int)((en-st)/1000));
            		_measurements.reportReturnCode("READ",res);
            		if (res != 0) {
            			System.err.println("READ failed!");
	        			break;
            		}
        		}
        	}
        	_transactionalDB.endTransaction();
        	
        	requests.clear();
        	
            _measurements.measure("TRANSACTION", (int)((System.nanoTime()-txStart)/1000));
            _measurements.reportReturnCode("TRANSACTION", 1);
        }
    }

	/**
	 * Set the properties for this DB.
	 */
	public void setProperties(Properties p)
	{
		_db.setProperties(p);
	}

	/**
	 * Get the set of properties for this DB.
	 */
	public Properties getProperties()
	{
		return _db.getProperties();
	}

	/**
	 * Initialize any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void init() throws DBException
	{
		requests = new ArrayList<Req>();
		_db.init();
	}

	/**
	 * Cleanup any state for this DB.
	 * Called once per DB instance; there is one DB instance per client thread.
	 */
	public void cleanup() throws DBException
	{
		_db.cleanup();
	}

	/**
	 * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to read.
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A HashMap of field/value pairs for the result
	 * @return Zero on success, a non-zero error code on error
	 */
	public int read(String table, String key, Set<String> fields, HashMap<String,ByteIterator> result)
	{
		requests.add(new GetReq(table, key, fields, result));
		checkTransaction();
		return 0;
	}
	
	public List<Req> requests;
	
	public static class GetReq extends Req {
		private final Set<String> fields;
		private final HashMap<String, ByteIterator> result;
		
		public GetReq(String table, String key, Set<String> fields,
				HashMap<String, ByteIterator> result) {
			super(table, key);
			this.fields = fields;
			this.result = result;
		}
		
		public Set<String> getFields() {
			return fields;
		}

		public HashMap<String, ByteIterator> getResult() {
			return result;
		}

		@Override
		public int compareTo(Req other) {
			int cmp = getKey().compareTo(other.getKey());
			if (cmp == 0) {
				return (other instanceof GetReq ? 0 : 1);
			}
			return cmp;
		}
	}
	
	public static class PutReq extends Req {
		private final HashMap<String, ByteIterator> values;
		
		public PutReq(String table, String key, 
				HashMap<String, ByteIterator> values) {
			super(table, key);
			this.values = values;
		}

		public HashMap<String, ByteIterator> getValues() {
			return values;
		}

		@Override
		public int compareTo(Req other) {
			int cmp = getKey().compareTo(other.getKey());
			if (cmp == 0) {
				return (other instanceof PutReq ? 0 : -1);
			}
			return cmp;
		}
	}
	
	public static class InsertReq extends PutReq {
		public InsertReq(String table, String key, 
				HashMap<String, ByteIterator> values) {
			super(table, key, values);
		}
	}
	
	public static class UpdateReq extends PutReq {
		public UpdateReq(String table, String key, 
				HashMap<String, ByteIterator> values) {
			super(table, key, values);
		}
	}
	
	public static abstract class Req implements Comparable<Req> {
		private final String table;
		private final String key;
		
		public Req(String table, String key) {
			this.table = table;
			this.key = key;
		}
		
		public String getTable() {
			return table;
		}
		
		public String getKey() {
			return key;
		}
	}

	/**
	 * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
	 *
	 * @param table The name of the table
	 * @param startkey The record key of the first record to read.
	 * @param recordcount The number of records to read
	 * @param fields The list of fields to read, or null for all of them
	 * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String,ByteIterator>> result)
	{
		long st=System.nanoTime();
        checkTransaction();
		int res=_db.scan(table,startkey,recordcount,fields,result);
        checkTransaction();
		long en=System.nanoTime();
		_measurements.measure("SCAN",(int)((en-st)/1000));
		_measurements.reportReturnCode("SCAN",res);
		return res;
	}
	
	/**
	 * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key, overwriting any existing values with the same field name.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to write.
	 * @param values A HashMap of field/value pairs to update in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int update(String table, String key, HashMap<String,ByteIterator> values)
	{
		requests.add(new UpdateReq(table, key, values));
		checkTransaction();
		return 0;
	}

	/**
	 * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
	 * record key.
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to insert.
	 * @param values A HashMap of field/value pairs to insert in the record
	 * @return Zero on success, a non-zero error code on error
	 */
	public int insert(String table, String key, HashMap<String,ByteIterator> values)
	{
		requests.add(new InsertReq(table, key, values));
		checkTransaction();
		return 0;
	}

	/**
	 * Delete a record from the database. 
	 *
	 * @param table The name of the table
	 * @param key The record key of the record to delete.
	 * @return Zero on success, a non-zero error code on error
	 */
	public int delete(String table, String key)
	{
		long st=System.nanoTime();
        checkTransaction();
		int res=_db.delete(table,key);
        checkTransaction();
		long en=System.nanoTime();
		_measurements.measure("DELETE",(int)((en-st)/1000));
		_measurements.reportReturnCode("DELETE",res);
		return res;
	}
}
