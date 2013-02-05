package edu.berkeley.thebes.hat.server;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigStrings;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.replica.AntiEntropyServiceHandler;
import edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;

public class ThebesHATServer {
    public static AntiEntropyServer antiEntropyServer;
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesHATServer.class);
    
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
        logger.debug("Starting the server...");
        ThriftServer.startInCurrentThread(
                new ReplicaService.Processor<ReplicaServiceHandler>(serviceHandler),
                Config.getServerBindIP());
    }

    public static void main(String[] args) {
        try {
            Log4JConfig.configureLog4J();
            Config.initializeServer(Config.TransactionMode.HAT);

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