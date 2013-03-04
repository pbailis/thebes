package edu.berkeley.thebes.hat.server.antientropy.clustering;


import com.google.common.collect.Lists;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.hat.common.clustering.RoutingHash;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class AntiEntropyServiceRouter {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceRouter.class);

    private static List<AntiEntropyService.Client> neighborClients = Lists.newArrayList();
    private static List<AntiEntropyService.AsyncClient> clusterClients = Lists.newArrayList();

    public void bootstrapAntiEntropyRouting() throws TTransportException {

        if (Config.isStandaloneServer()) {
            logger.debug("Server marked as standalone; not starting anti-entropy!");
            return;
        }

        try {
            Thread.sleep(Config.getAntiEntropyBootstrapTime());
        } catch (Exception e) {
        }

        logger.debug("Bootstrapping anti-entropy...");

        for (ServerAddress neighbor : Config.getSiblingServers()) {
            while (true) {
                try {
                    neighborClients.add(
                            ThriftUtil.getAntiEntropyServiceClient(neighbor.getIP(),
                                                                   Config.getAntiEntropyServerPort()));
                    break;
                } catch (Exception e) {
                    logger.error("Exception while bootstrapping connection with neighbor: " +
                                 neighbor.getIP() + ":" + Config.getAntiEntropyServerPort());
                    e.printStackTrace();
                }
            }
        }

        for (ServerAddress clusterServer : Config.getServersInCluster()) {
            while (true) {
                try {
                    clusterClients.add(
                            ThriftUtil.getAntiEntropyServiceAsyncClient(clusterServer.getIP(),
                                                                   Config.getAntiEntropyServerPort()));
                    break;
                } catch (Exception e) {
                    logger.error("Exception while bootstrapping connection with cluster server: " +
                                 clusterServer.getIP() + ":" + Config.getAntiEntropyServerPort());
                    e.printStackTrace();
                }
            }
        }

        logger.debug("...anti-entropy bootstrapped");
    }

    public AntiEntropyService.AsyncClient getReplicaByKey(String key) {
        return clusterClients.get(RoutingHash.hashKey(key, clusterClients.size()));
    }

    public List<AntiEntropyService.Client> getNeighborClients() {
        return neighborClients;
    }
}