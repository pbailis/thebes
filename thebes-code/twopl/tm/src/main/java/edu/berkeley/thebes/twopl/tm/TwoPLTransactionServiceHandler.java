package edu.berkeley.thebes.twopl.tm;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;
import org.apache.thrift.TException;

import java.util.List;

public class TwoPLTransactionServiceHandler implements TwoPLTransactionService.Iface {
    private IPersistenceEngine persistenceEngine;
    private TwoPLTransactionClient client;
    private TwoPLOperationInterpreter interpreter;

    public TwoPLTransactionServiceHandler(IPersistenceEngine persistenceEngine,
            TwoPLTransactionClient client) {
        this.persistenceEngine = persistenceEngine;
        this.client = client;
        this.interpreter = new SimpleStackOperationInterpreter(client);
    }

    @Override
    public TwoPLTransactionResult execute(List<String> transaction) throws TException {
        client.beginTransaction();
        try {
            for (String operation : transaction) {
                interpreter.execute(operation);
            }
        } catch (AssertionError e) {
            throw new TTransactionAbortedException(e.getMessage());
        } finally {
            client.endTransaction();
        }
        
        return new TwoPLTransactionResult(interpreter.getOutput());
    }
}