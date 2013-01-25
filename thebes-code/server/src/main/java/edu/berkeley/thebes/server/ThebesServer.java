package edu.berkeley.thebes.server;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigStrings;
import edu.berkeley.thebes.common.thrift.ThebesReplicaService;
import edu.berkeley.thebes.server.persistence.IPersistenceEngine;
import edu.berkeley.thebes.server.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.server.replica.ThebesReplicaServiceHandler;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;

import javax.naming.ConfigurationException;

public class ThebesServer {
    public static void startThebesServer(ThebesReplicaService.Processor<ThebesReplicaServiceHandler> processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(Config.getServerPort());
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(processor));

            System.out.println("Starting the simple server...");
            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        try {
            Config.initializeServerConfig(args);

            IPersistenceEngine engine;

            String engineType = Config.getPersistenceType();
            if(engineType.equals(ConfigStrings.PERSISTENCE_MEMORY))
                engine = new MemoryPersistenceEngine();
            else
                throw new ConfigurationException("unexpected persistency type: "+engineType);

            engine.open();


            startThebesServer(new ThebesReplicaService.Processor<ThebesReplicaServiceHandler>
                                      (new ThebesReplicaServiceHandler(engine)));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}