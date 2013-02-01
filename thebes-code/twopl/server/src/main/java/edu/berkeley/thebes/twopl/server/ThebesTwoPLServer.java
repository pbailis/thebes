package edu.berkeley.thebes.twopl.server;

import javax.naming.ConfigurationException;

import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigStrings;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.AntiEntropyServer;
import edu.berkeley.thebes.hat.server.replica.AntiEntropyServiceHandler;
import edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;

public class ThebesTwoPLServer {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesTwoPLServer.class);
    
    public static void startTwoPLServer(TwoPLMasterServiceHandler serviceHandler) {
        try {
            TwoPLMasterReplicaService.Processor<TwoPLMasterServiceHandler> processor =
                    new TwoPLMasterReplicaService.Processor<TwoPLMasterServiceHandler>(serviceHandler);
            
            TServerTransport serverTransport = new TServerSocket(Config.getTwoPLServerBindIP());
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
            Log4JConfig.configureLog4J();
            Config.initializeServer(Config.TransactionMode.TWOPL);

            IPersistenceEngine engine;

            String engineType = Config.getPersistenceType();
            if (engineType.equals(ConfigStrings.PERSISTENCE_MEMORY))
                engine = new MemoryPersistenceEngine();
            else
                throw new ConfigurationException("unexpected persistency type: " + engineType);

            engine.open();

            TwoPLLocalLockManager lockManager = new TwoPLLocalLockManager();
            startTwoPLServer(new TwoPLMasterServiceHandler(engine, lockManager));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}