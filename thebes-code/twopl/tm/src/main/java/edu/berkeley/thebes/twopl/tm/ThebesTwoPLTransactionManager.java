package edu.berkeley.thebes.twopl.tm;

import javax.naming.ConfigurationException;

import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigStrings;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLTransactionService;

public class ThebesTwoPLTransactionManager {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesTwoPLTransactionManager.class);

    public static void startServer(TwoPLTransactionServiceHandler serviceHandler) {
        logger.debug("Starting transaction manager...");

        ThriftServer.startInCurrentThread(
                new TwoPLTransactionService.Processor<TwoPLTransactionServiceHandler>(serviceHandler),
                Config.getTwoPLTransactionManagerBindIP());
    }
    
    public static void main(String[] args) {
        try {
            Log4JConfig.configureLog4J();
            Config.initializeTwoPLTransactionManager();

            IPersistenceEngine engine;

            String engineType = Config.getPersistenceType();
            if (engineType.equals(ConfigStrings.PERSISTENCE_MEMORY))
                engine = new MemoryPersistenceEngine();
            else
                throw new ConfigurationException("unexpected persistency type: " + engineType);

            engine.open();
            
            TwoPLTransactionClient client = new TwoPLTransactionClient();
            client.open();
            startServer(new TwoPLTransactionServiceHandler(engine, client));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}