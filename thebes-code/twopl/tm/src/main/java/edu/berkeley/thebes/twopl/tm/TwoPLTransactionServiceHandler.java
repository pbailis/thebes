package edu.berkeley.thebes.twopl.tm;

import java.util.List;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.thrift.TTransactionAbortedException;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionResult;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;

public class TwoPLTransactionServiceHandler implements TwoPLTransactionService.Iface {
    private TwoPLTransactionClient client;

    public TwoPLTransactionServiceHandler(TwoPLTransactionClient client) {
        this.client = client;
    }

    @Override
    public TwoPLTransactionResult execute(List<String> transaction) throws TException {
        TwoPLOperationInterpreter interpreter = new SimpleStackOperationInterpreter(client); 
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