package edu.berkeley.thebes.twopl.server;

import javax.naming.ConfigurationException;

import org.apache.log4j.BasicConfigurator;
import org.apache.thrift.TProcessor;
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
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.AntiEntropyServer;
import edu.berkeley.thebes.hat.server.replica.AntiEntropyServiceHandler;
import edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLSlaveReplicaService;

public class ThebesTwoPLServer {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesTwoPLServer.class);

    private static TwoPLSlaveReplicationService slaveReplicationService;
    
    public static void startMasterServer(TwoPLMasterServiceHandler serviceHandler) {
        logger.debug("Starting master server...");

        // Connect to slaves to replicate to them.
        new Thread() {
            @Override
            public void run() {
                slaveReplicationService.connectSlaves();
            }
        }.start();
        
        ThriftServer.startInCurrentThread(
                new TwoPLMasterReplicaService.Processor<TwoPLMasterServiceHandler>(serviceHandler),
                Config.getTwoPLServerBindIP());
    }
    
    public static void startSlaveServer(TwoPLSlaveServiceHandler serviceHandler) {
        logger.debug("Starting slave server...");
        ThriftServer.startInCurrentThread(
                new TwoPLSlaveReplicaService.Processor<TwoPLSlaveServiceHandler>(serviceHandler),
                Config.getTwoPLServerBindIP());
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
            slaveReplicationService = new TwoPLSlaveReplicationService();
            if (Config.isMaster()) {
                startMasterServer(new TwoPLMasterServiceHandler(engine, lockManager,
                        slaveReplicationService));
            } else {
                startSlaveServer(new TwoPLSlaveServiceHandler(engine)); 
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}