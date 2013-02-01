package edu.berkeley.thebes.hat.common.thrift;

import edu.berkeley.thebes.common.config.Config;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class ThriftUtil {
    public static ReplicaService.Client getReplicaServiceClient(
            String host, int port, int timeout) throws TTransportException {
        TTransport transport;

        transport = new TSocket(host, port, timeout);
        transport.open();

        TProtocol protocol = new TBinaryProtocol(transport);

        //by not returning the transport, we're not allowing the user to close() it
        //todo?
        return new ReplicaService.Client(protocol);
    }

    public static ReplicaService.Client getReplicaServiceClient(
            String host, int port) throws TTransportException {
        return getReplicaServiceClient(host, port, Config.getSocketTimeout());
    }
    
    public static AntiEntropyService.Client getAntiEntropyServiceClient(
            String host, int port, int timeout) throws TTransportException {
        TTransport transport;

        transport = new TSocket(host, port, timeout);
        transport.open();

        TProtocol protocol = new TBinaryProtocol(transport);

        //by not returning the transport, we're not allowing the user to close() it
        //todo?
        return new AntiEntropyService.Client(protocol);
    }

    public static AntiEntropyService.Client getAntiEntropyServiceClient(
            String host, int port) throws TTransportException {
        return getAntiEntropyServiceClient(host, port, Config.getSocketTimeout());
    }
}