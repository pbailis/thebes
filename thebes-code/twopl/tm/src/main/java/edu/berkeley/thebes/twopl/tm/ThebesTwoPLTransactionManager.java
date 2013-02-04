package edu.berkeley.thebes.twopl.tm;

import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
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

            TwoPLTransactionClient client = new TwoPLTransactionClient();
            client.open();
            startServer(new TwoPLTransactionServiceHandler(client));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}