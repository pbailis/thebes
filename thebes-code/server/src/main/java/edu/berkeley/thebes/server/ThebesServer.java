package edu.berkeley.thebes.server;

import edu.berkeley.thebes.common.thrift.ThebesReplicaService;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

public class ThebesServer 
{
    public static void StartThebesServer(ThebesReplicaService.Processor<ThebesReplicaServiceHandler> processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(9090);
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main( String[] args )
    {
        StartThebesServer(new ThebesReplicaService.Processor<ThebesReplicaServiceHandler>
                (new ThebesReplicaServiceHandler()));
        System.out.println( "Hello World!" );
    }
}