package edu.berkeley.thebes.twopl.common.thrift;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class TwoPLThriftUtil {
    public static TwoPLMasterReplicaService.Client getMasterReplicaServiceClient(
            String host, int port) throws TTransportException {
        TProtocol protocol = createProtocol(host, port, Config.getSocketTimeout());
        return new TwoPLMasterReplicaService.Client(protocol);
    }
    
    private static TProtocol createProtocol(String host, int port, int timeout)
            throws TTransportException {
        TTransport transport = new TSocket(host, port, timeout);
        transport.open();
        return new TBinaryProtocol(transport);
    }
}