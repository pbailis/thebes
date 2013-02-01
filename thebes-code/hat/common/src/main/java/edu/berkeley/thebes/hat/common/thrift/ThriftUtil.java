package edu.berkeley.thebes.hat.common.thrift;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftUtil {
    public static ReplicaService.Client getReplicaServiceClient(
            String host, int port) throws TTransportException {
        TProtocol protocol = createProtocol(host, port, Config.getSocketTimeout());
        return new ReplicaService.Client(protocol);
    }

    public static AntiEntropyService.Client getAntiEntropyServiceClient(
            String host, int port) throws TTransportException {
        TProtocol protocol = createProtocol(host, port, Config.getSocketTimeout());
        return new AntiEntropyService.Client(protocol);
    }

    public static TwoPLMasterReplicaService.Client getTwoPLMasterReplicaServiceClient(
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