package edu.berkeley.thebes.server;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigStrings;
import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.server.persistence.IPersistenceEngine;
import edu.berkeley.thebes.server.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.server.replica.ReplicaServiceHandler;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.slf4j.LoggerFactory;

import javax.naming.ConfigurationException;

public class ThebesServer {
    public static AntiEntropyService antiEntropyService;
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(AntiEntropyService.class);

    public static void startThebesServer(ReplicaService.Processor<ReplicaServiceHandler> processor) {
        try {
            TServerTransport serverTransport = new TServerSocket(Config.getServerBindIP());
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(processor));

            antiEntropyService = new AntiEntropyService();
            if (!Config.isStandaloneServer()) {
                (new Thread(antiEntropyService)).start();
            } else {
                logger.debug("Server marked as standalone; not starting anti-entropy!");
            }

            logger.debug("Starting the server...");

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
            if (engineType.equals(ConfigStrings.PERSISTENCE_MEMORY))
                engine = new MemoryPersistenceEngine();
            else
                throw new ConfigurationException("unexpected persistency type: " + engineType);

            engine.open();


            startThebesServer(new ReplicaService.Processor<ReplicaServiceHandler>
                                      (new ReplicaServiceHandler(engine)));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}