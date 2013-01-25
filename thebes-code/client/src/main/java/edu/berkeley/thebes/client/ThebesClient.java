package edu.berkeley.thebes.client;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ThebesReplicaService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

import java.nio.ByteBuffer;

public class ThebesClient {
    public static void main(String[] args) {
        try {
            Config.initializeClientConfig(args);

            TTransport transport;

            transport = new TSocket("localhost", 8080);
            transport.open();

            TProtocol protocol = new TBinaryProtocol(transport);
            ThebesReplicaService.Client client = new ThebesReplicaService.Client(protocol);

            System.out.println(client.put("foo", ByteBuffer.wrap("foobar".getBytes())));
            ByteBuffer ret = client.get("foo");
            System.out.println(new String(ret.array()));

            transport.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}