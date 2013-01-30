package edu.berkeley.thebes.server;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigStrings;
import edu.berkeley.thebes.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.server.persistence.IPersistenceEngine;
import edu.berkeley.thebes.server.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.server.replica.AntiEntropyServiceHandler;
import edu.berkeley.thebes.server.replica.ReplicaServiceHandler;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;

public class ThebesServer {
    public static AntiEntropyServer antiEntropyServer;
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(AntiEntropyServer.class);
    
    public static AntiEntropyServer startAntiEntropyServer(
            AntiEntropyServiceHandler serviceHandler) {
        antiEntropyServer = new AntiEntropyServer(serviceHandler);
        if (!Config.isStandaloneServer()) {
            (new Thread(antiEntropyServer)).start();
            antiEntropyServer.connectNeighbors();
        } else {
            logger.debug("Server marked as standalone; not starting anti-entropy!");
        }
        return antiEntropyServer;
    }

    public static void startThebesServer(ReplicaServiceHandler serviceHandler) {
        try {
            ReplicaService.Processor<ReplicaServiceHandler> processor =
                    new ReplicaService.Processor<ReplicaServiceHandler>(serviceHandler);
            
            TServerTransport serverTransport = new TServerSocket(Config.getServerBindIP());
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(processor));

            logger.debug("Starting the server...");

            server.serve();
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        try {
            Config.initializeServerConfig(args);

            IPersistenceEngine engine;

            String engineType = Config.getPersistenceType();
            if (engineType.equals(ConfigStrings.PERSISTENCE_MEMORY))
                engine = new MemoryPersistenceEngine();
            else
                throw new ConfigurationException("unexpected persistency type: " + engineType);

            engine.open();

            AntiEntropyServer antiEntropyServer = 
                    startAntiEntropyServer(new AntiEntropyServiceHandler(engine));
            startThebesServer(new ReplicaServiceHandler(engine, antiEntropyServer));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}