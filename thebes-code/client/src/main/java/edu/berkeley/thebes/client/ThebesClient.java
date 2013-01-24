package edu.berkeley.thebes.client;

import edu.berkeley.thebes.common.thrift.ThebesReplicaService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.nio.ByteBuffer;

public class ThebesClient 
{
    public static void main( String[] args )
    {
        try {
            TTransport transport;

            transport = new TSocket("localhost", 9090);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            ThebesReplicaService.Client client = new ThebesReplicaService.Client(protocol);

            System.out.println(client.put("foo", ByteBuffer.allocate(0)));
            System.out.println(client.get("foo"));

            transport.close();
        } catch (TTransportException e) {
            e.printStackTrace();
        } catch (TException x) {
            x.printStackTrace();
        }
    }
}