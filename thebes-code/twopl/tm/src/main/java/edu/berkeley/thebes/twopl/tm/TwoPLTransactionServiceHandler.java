package edu.berkeley.thebes.twopl.tm;

import java.util.List;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;

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
        String errorOutput = null;
        try {
            for (String operation : transaction) {
                interpreter.execute(operation);
            }
        } catch (AssertionError e) {
            errorOutput = e.getMessage();
        } finally {
            client.endTransaction();
        }
        
        return new TwoPLTransactionResult(errorOutput == null,
                interpreter.getOutput(), errorOutput);
    }
}