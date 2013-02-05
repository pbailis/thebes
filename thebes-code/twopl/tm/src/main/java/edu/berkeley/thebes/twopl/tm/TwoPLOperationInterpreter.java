package edu.berkeley.thebes.twopl.tm;

import org.apache.thrift.TException;

import java.nio.ByteBuffer;
import java.util.Map;

public interface TwoPLOperationInterpreter {    
    /** Parses and executes the given operation! 
     * @return 
     * @throws TException */
    ByteBuffer execute(String operation) throws TException;
    
    Map<String, ByteBuffer> getOutput();
}
