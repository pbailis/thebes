package edu.berkeley.thebes.hat.server.antientropy.clustering;


import com.google.common.collect.Maps;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.DataDependencyRequest;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

public class AntiEntropyServiceRouter {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceRouter.class);

    private static List<AntiEntropyService.Client> neighborClients = Lists.newArrayList();
    private static List<AntiEntropyService.Client> waitClusterClients = Lists.newArrayList();
    private static List<AntiEntropyService.Client> notifyClusterClients = Lists.newArrayList();
    private static int numServersInCluster;

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

        numServersInCluster = Config.getServersInCluster().size();

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

        for (ServerAddress neighbor : Config.getServersInCluster()) {
            while (true) {
                try {
                    notifyClusterClients.add(
                            ThriftUtil.getAntiEntropyServiceClient(neighbor.getIP(),
                                                                   Config.getAntiEntropyServerPort()));
                    waitClusterClients.add(
                            ThriftUtil.getAntiEntropyServiceClient(neighbor.getIP(),
                                                                   Config.getAntiEntropyServerPort()));
                    break;
                } catch (Exception e) {
                    logger.error("Exception while bootstrapping connection with cluster server: " +
                                 neighbor.getIP() + ":" + Config.getAntiEntropyServerPort());
                    e.printStackTrace();
                }
            }
        }

        logger.debug("...anti-entropy bootstrapped");

        startDependencyResolverSendingThread();
        startDependencyResolvedResponseThread();
    }

    /*
    public AntiEntropyService.AsyncClient getAsyncClient(String key) throws TTransportException, IOException {
        return clientPool.getClientByKey(key);
    }

    public void returnAsyncClient(String key, AntiEntropyService.AsyncClient client) {
        clientPool.returnClient(key, client);
    }
    */

    private class QueuedDependency {
        private String key;
        private DataDependency dependency;
        private DependencyResolver.WaitingResolvedDependency waiting;

        public QueuedDependency(String key, DataDependency dependency, DependencyResolver.WaitingResolvedDependency waiting) {
            this.key = key;
            this.dependency = dependency;
            this.waiting = waiting;
        }

        public DataDependency getDependency() {
            return dependency;
        }

        public void notifyFinished() {
            waiting.notifyResolved();
        }

        public String getKey() {
            return key;
        }
    }


    /*
     Code to handle dependency request responses
     server with data item -> requestor
     */

    private LinkedBlockingQueue<DataDependencyRequest> completedRequests = new LinkedBlockingQueue
            <DataDependencyRequest>();

    public void sendDependencyResolved(DataDependencyRequest request) {
        completedRequests.add(request);
    }

    private void startDependencyResolvedResponseThread() {
        Map<Integer, List<Long>> toSend = Maps.newHashMap();

        new Thread(new Runnable() {
           @Override
           public void run() {
               while(true) {
                   Map<Integer, List<Long>> toSend = Maps.newHashMap();
                   for(int i = 0; i < numServersInCluster; ++i) {
                       toSend.put(i, new ArrayList<Long>());
                   }

                   try {
                       DataDependencyRequest dependency = completedRequests.take();
                       toSend.get(dependency.getServerId()).add(dependency.getRequestId());

                       int dqSize = completedRequests.size();

                       logger.debug("Sending "+dqSize+" dependency responses");

                       for(int i = 0; i < dqSize; ++i) {
                           dependency = completedRequests.take();
                           toSend.get(dependency.getServerId()).add(dependency.getRequestId());
                       }

                       for(int i = 0; i < numServersInCluster; ++i) {
                           List<Long> queuedDependencies = toSend.get(i);

                           if(queuedDependencies.size() > 0)
                               notifyClusterClients.get(i).receiveTransactionalDependencies(queuedDependencies);
                       }

                   } catch (Exception e) {
                       logger.warn("error: ", e);
                   }
               }

           }
       }).start();
   }
    /*
      Code to handle outgoing dependency requests
      requestor -> server with data item
     */

    private LinkedBlockingQueue<QueuedDependency> dependencyQueue = new LinkedBlockingQueue<QueuedDependency>();

    public void waitForDependencyRemote(String key, DataDependency dependency, DependencyResolver.WaitingResolvedDependency waiting) {
        dependencyQueue.add(new QueuedDependency(key, dependency,  waiting));
    }

    private Map<Long, QueuedDependency> waitingRequests = Maps.newConcurrentMap();

    public void resolvedDependencyArrived(long resolvedRequestId) {
        if(!waitingRequests.containsKey(resolvedRequestId))
            logger.error("Got resolved request with ID "+resolvedRequestId+" but not present!");

        waitingRequests.remove(resolvedRequestId).notifyFinished();
    }

    private void startDependencyResolverSendingThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                long seqNo = 0;
                final int myServerId = Config.getServerID();

                while(true) {
                    Map<Integer, List<DataDependencyRequest>> toSend = Maps.newHashMap();
                    for(int i = 0; i < numServersInCluster; ++i) {
                        toSend.put(i, new ArrayList<DataDependencyRequest>());
                    }

                    try {
                        QueuedDependency dependency = dependencyQueue.take();
                        toSend.get(RoutingHash.hashKey(dependency.key, numServersInCluster))
                              .add(new DataDependencyRequest(myServerId,
                                                             seqNo,
                                                             DataDependency.toThrift(dependency.getDependency())));

                        waitingRequests.put(seqNo, dependency);
                        seqNo++;

                        int dqSize = dependencyQueue.size();

                        logger.debug("Sending "+dqSize+" dependency requests (waitingSize=" + waitingRequests.size() + ")");

                        for(int i = 0; i < dqSize; ++i) {
                            dependency = dependencyQueue.take();
                            toSend.get(RoutingHash.hashKey(dependency.key, numServersInCluster))
                                  .add(new DataDependencyRequest(myServerId,
                                                                 seqNo,
                                                                 DataDependency.toThrift(dependency.getDependency())));
                            waitingRequests.put(seqNo, dependency);
                            seqNo++;
                        }

                        for(int i = 0; i < numServersInCluster; ++i) {
                            List<DataDependencyRequest> queuedDependencies = toSend.get(i);

                            if(queuedDependencies.size() > 0)
                                waitClusterClients.get(i).waitForTransactionalDependencies(queuedDependencies);
                        }

                    } catch (Exception e) {
                        logger.warn("error: ", e);
                    }
                }

            }
        }).start();
    }

    public List<AntiEntropyService.Client> getNeighborClients() {
        return neighborClients;
    }
}