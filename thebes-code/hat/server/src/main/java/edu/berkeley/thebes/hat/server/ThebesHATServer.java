package edu.berkeley.thebes.hat.server;

import javax.naming.ConfigurationException;

import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServer;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import edu.berkeley.thebes.hat.server.dependencies.GenericDependencyChecker;
import edu.berkeley.thebes.hat.server.dependencies.GenericDependencyResolver;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.TransactionMode;
import edu.berkeley.thebes.common.log4j.Log4JConfig;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.persistence.memory.MemoryPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.server.antientropy.AntiEntropyServiceHandler;
import edu.berkeley.thebes.hat.server.replica.ReplicaServiceHandler;

public class ThebesHATServer {
    private static org.slf4j.Logger logger = LoggerFactory.getLogger(ThebesHATServer.class);

    public static void main(String[] args) {
        try {
            Log4JConfig.configureLog4J();
            Config.initializeServer(TransactionMode.HAT);

            IPersistenceEngine engine;

            PersistenceEngine engineType = Config.getPersistenceType();
            switch (engineType) {
            case MEMORY:
                engine = new MemoryPersistenceEngine();
                break;
            default:
                throw new ConfigurationException("unexpected persistency type: " + engineType);
            }
            engine.open();

            AntiEntropyServiceRouter router = new AntiEntropyServiceRouter();

            GenericDependencyResolver causalDependencyResolver = new GenericDependencyResolver(engine);
            GenericDependencyChecker causalDependencyChecker = new GenericDependencyChecker(engine,
                                                                                            router,
                                                                                            causalDependencyResolver);

            AntiEntropyServiceHandler antiEntropyServiceHandler = new AntiEntropyServiceHandler(engine,
                                                                                                causalDependencyChecker,
                                                                                                causalDependencyResolver);
            AntiEntropyServer antiEntropyServer = new AntiEntropyServer(antiEntropyServiceHandler);

            if (!Config.isStandaloneServer()) {
                (new Thread(antiEntropyServer)).start();
            } else {
                logger.debug("Server marked as standalone; not starting anti-entropy!");
            }

            ReplicaServiceHandler replicaServiceHandler = new ReplicaServiceHandler(engine, antiEntropyServer, causalDependencyResolver);

            logger.debug("Starting the server...");
            ThriftServer.startInCurrentThread(
                    new ReplicaService.Processor<ReplicaServiceHandler>(replicaServiceHandler),
                    Config.getServerBindIP());

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}