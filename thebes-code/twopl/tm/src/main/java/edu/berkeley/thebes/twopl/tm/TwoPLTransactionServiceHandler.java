package edu.berkeley.thebes.twopl.tm;

import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.ThebesTwoPLTransactionClient;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;
import edu.berkeley.thebes.twopl.tm.SimpleStackOperationInterpreter.Function;
import edu.berkeley.thebes.twopl.tm.SimpleStackOperationInterpreter.StatementNode;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

public class TwoPLTransactionServiceHandler implements TwoPLTransactionService.Iface {
    private ThebesTwoPLTransactionClient client;

    public TwoPLTransactionServiceHandler(ThebesTwoPLTransactionClient client) {
        this.client = client;
    }

    @Override
    public synchronized TwoPLTransactionResult execute(List<String> transaction) throws TException {
        SimpleStackOperationInterpreter interpreter =
                new SimpleStackOperationInterpreter(client);
        
        // Parse the transaction and determine which keys are read to & written from
        List<StatementNode> statements = Lists.newArrayList();
        Set<String> readKeys = Sets.newHashSet();
        Set<String> writeKeys = Sets.newHashSet();
        for (String operation : transaction) {
        	if (operation.startsWith("put")) {
        		String key = operation.substring(operation.indexOf(" ")+1, operation.lastIndexOf(" "));
        		String value = operation.substring(operation.lastIndexOf(" ")+1);
        		readKeys.remove(key);
        		writeKeys.add(key);
        	} else {
        		String key = operation.substring(operation.indexOf(" ")+1);
                if (!writeKeys.contains(key)) {
                    readKeys.add(key);
                }
        	}
        	/*
            StatementNode statement = interpreter.parse(operation);
            String key = statement.getTarget();
            
            if (statement.getFunction() == Function.PUT) {
                readKeys.remove(key);
                writeKeys.add(key);
            } else if (statement.getFunction() == Function.GET) {
                if (!writeKeys.contains(key)) {
                    readKeys.add(key);
                }
            }
            
            statements.add(statement);
            */
        }
        
        // Now actually execute commands, starting by locking everything we need.
        client.beginTransaction();
        try {
            for (String writeKey : writeKeys) {
                client.writeLock(writeKey);
            }
            for (String readKey : readKeys) {
                client.readLock(readKey);
            }
            for (String operation : transaction) {
            	if (operation.startsWith("put")) {
            		String key = operation.substring(operation.indexOf(" ")+1, operation.lastIndexOf(" "));
            		String value = operation.substring(operation.lastIndexOf(" ")+1);
            		client.put(key, ByteBuffer.wrap(value.getBytes()));
            	} else {
            		String key = operation.substring(operation.indexOf(" ")+1);
                    client.get(key);
            	}
            }
            /*
            for (StatementNode statement : statements) {
                // TODO: Clean up interpreter by having it merely evaluate, not execute.
                interpreter.execute(statement);
            }*/
        } catch (AssertionError e) {
            e.printStackTrace();
            throw new TTransactionAbortedException(e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            client.endTransaction();
        }
        
        return new TwoPLTransactionResult(interpreter.getOutput());
    }
}