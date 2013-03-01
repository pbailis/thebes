package com.yahoo.ycsb.db;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Set;
import java.util.Vector;

import com.yahoo.ycsb.ByteArrayByteIterator;
import com.yahoo.ycsb.StringByteIterator;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import org.apache.log4j.Logger;

import edu.berkeley.thebes.client.ThebesClient;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.TransactionFinished;


public class ThebesYCSBClient extends DB implements TransactionFinished {

    ThebesClient client;

    private final Logger logger = Logger.getLogger(ThebesYCSBClient.class);
    
    public static final int OK = 0;
    public static final int ERROR = -1;
    public static final int NOT_FOUND = -2;

    //TOFIX!
    private static IntegerGenerator transactionLengthGenerator = new ConstantIntegerGenerator(4);

    private int currentTransactionLength = -1;
    private int finalTransactionLength = -1;

    private void checkStartTransaction() {
        if(currentTransactionLength < finalTransactionLength) {
            currentTransactionLength++;
        }
        else {
            if(finalTransactionLength != -1)
                endTransaction();

            finalTransactionLength = transactionLengthGenerator.nextInt();
            beginTransaction();
            currentTransactionLength = 0;
        }
    }

    public boolean transactionFinished() {
        return currentTransactionLength == 0;
    }
    
	public void init() throws DBException {
        String transactionLengthDistributionType = System.getProperty("transactionLengthDistributionType");
        String transactionLengthDistributionParameter = System.getProperty("transactionLengthDistributionParameter");
        if(transactionLengthDistributionType == null)
            throw new DBException("required transactionLengthDistributionType");

        if(transactionLengthDistributionParameter == null)
            throw new DBException("requried transactionLengthDistributionParameter");

        if(transactionLengthDistributionType.compareTo("constant") == 0) {
            transactionLengthGenerator = new ConstantIntegerGenerator(Integer.parseInt(transactionLengthDistributionParameter));
        }
        else {
            throw new DBException("unimplemented transactionLengthDistribution type!");
        }

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
            checkStartTransaction();
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
            checkStartTransaction();
            client.put(key, ByteBuffer.wrap(values.values().iterator().next().toArray()));
        } catch (Exception e) {
            logger.warn(e.getMessage());
            e.printStackTrace();
            return ERROR;
        }
		return OK;
	}

	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) {
        try {
            checkStartTransaction();
            ByteBuffer ret = client.get(key);



            if(ret == null || ret.array() == null) {
                result.put(fields.iterator().next(), null);
                return OK;
            }

            if(fields != null)
                result.put(fields.iterator().next(), new ByteArrayByteIterator(ret.array()));
            else
                result.put("value", new ByteArrayByteIterator(ret.array()));


        } catch (Exception e) {
            logger.warn(e.getMessage());
            e.printStackTrace();
            return ERROR;
        }
		return OK;
	}

	@Override
	public int scan(String table, String startkey, int recordcount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        checkStartTransaction();
        logger.warn("Thebes scans are not implemented!");
		return ERROR;
	}

	@Override
	public int update(String table, String key, HashMap<String, ByteIterator> values) {
        //update doesn't pass in the entire record, so we'd need to do read-modify-write
        return insert(table, key,  values);
	}
	
	private int checkStore(String table) {
		return OK;
	}
}
