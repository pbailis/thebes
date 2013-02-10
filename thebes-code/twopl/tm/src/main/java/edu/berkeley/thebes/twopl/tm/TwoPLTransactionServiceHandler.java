package edu.berkeley.thebes.twopl.tm;

import org.apache.thrift.TException;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;
import edu.berkeley.thebes.twopl.tm.SimpleStackOperationInterpreter.Function;
import edu.berkeley.thebes.twopl.tm.SimpleStackOperationInterpreter.StatementNode;

import java.util.List;
import java.util.Set;

public class TwoPLTransactionServiceHandler implements TwoPLTransactionService.Iface {
    private TwoPLTransactionClient client;

    public TwoPLTransactionServiceHandler(TwoPLTransactionClient client) {
        this.client = client;
    }

    @Override
    public TwoPLTransactionResult execute(List<String> transaction) throws TException {
        SimpleStackOperationInterpreter interpreter =
                new SimpleStackOperationInterpreter(client);
        
        // Parse the transaction and determine which keys are read to & written from
        List<StatementNode> statements = Lists.newArrayList();
        Set<String> readKeys = Sets.newHashSet();
        Set<String> writeKeys = Sets.newHashSet();
        for (String operation : transaction) {
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
        }
        
        // Now actually execute commands, starting by locking everything we need.
        client.beginTransaction();
        try {
            for (String writeKey : writeKeys) {
                assert client.writeLock(writeKey) : "Failed to get write lock: " + writeKey;
            }
            for (String readKey : readKeys) {
                assert client.readLock(readKey) : "Failed to get read lock: " + readKey;
            }
            for (StatementNode statement : statements) {
                // TODO: Clean up interpreter by having it merely evaluate, not execute.
                interpreter.execute(statement);
            }
        } catch (AssertionError e) {
            throw new TTransactionAbortedException(e.getMessage());
        } finally {
            client.endTransaction();
        }
        
        return new TwoPLTransactionResult(interpreter.getOutput());
    }
}