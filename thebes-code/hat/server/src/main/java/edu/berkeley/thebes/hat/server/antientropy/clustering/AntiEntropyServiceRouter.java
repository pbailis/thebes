package edu.berkeley.thebes.hat.server.antientropy.clustering;


import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;
import edu.berkeley.thebes.hat.server.dependencies.PendingWrite;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AntiEntropyServiceRouter {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceRouter.class);

    /** Siblings replicate the same data. */
    private static List<AntiEntropyService.Client> replicaSiblingClients;
    /** Neighbors live in the same cluster. Includes self! */
    private static List<AntiEntropyService.Client> neighborClients;
    private static int numServersInCluster;

    public void bootstrapAntiEntropyRouting() throws TTransportException {
        if (Config.isStandaloneServer()) {
            logger.debug("Server marked as standalone; not starting anti-entropy!");
            // TODO: Fix this.
            throw new IllegalArgumentException("Standalone mode disabled: Need anti-entropy for TA");
        }

        Uninterruptibles.sleepUninterruptibly(Config.getAntiEntropyBootstrapTime(),
                TimeUnit.MILLISECONDS);

        logger.debug("Bootstrapping anti-entropy...");

        numServersInCluster = Config.getServersInCluster().size();
        replicaSiblingClients = createClientsFromAddresses(Config.getSiblingServers());
        neighborClients = createClientsFromAddresses(Config.getServersInCluster());

        logger.debug("...anti-entropy bootstrapped");

        logger.trace("Starting thread to forward writes to siblings...");
        new Thread() {
            public void run() {
                while (true) {
                    forwardNextQueuedWriteToSiblings();
                }
            }
        }.start();

        logger.trace("Starting thread to announce new pending writes...");
        new Thread() {
            public void run() {
                while (true) {
                    announceNextQueuedPendingWrite();
                }
            }
        }.start();
    }

    /** Stores the writes we receive and need to forward to all siblings */
    private final LinkedBlockingQueue<QueuedWrite> writesToForwardSiblings;
    /** Stores the writes we've put into pending, and need to notify all dependent neighbors. */
    private final LinkedBlockingQueue<PendingWrite> pendingWritesToAnnounce;
    
    public AntiEntropyServiceRouter() {
        this.writesToForwardSiblings = Queues.newLinkedBlockingQueue();
        this.pendingWritesToAnnounce = Queues.newLinkedBlockingQueue();
    }
    
    /** Our cluster got a new write, forward to the replicas in other clusters. */
    public void sendWriteToSiblings(String key, ThriftDataItem value) {
        writesToForwardSiblings.add(new QueuedWrite(key, value));
    }

    /** Actually does the forwarding! Called in its own thread. */
    private void forwardNextQueuedWriteToSiblings() {
        try {
            QueuedWrite writeToForward = 
                    Uninterruptibles.takeUninterruptibly(writesToForwardSiblings);
            for (AntiEntropyService.Client sibling : replicaSiblingClients) {
                sibling.put(writeToForward.key, writeToForward.value);
            }
        } catch (TException e) {
            logger.error("Failure while announcing queued pending write: ", e);
        }
    }

    /** Our replica got a write, announce it to others in this cluster who depend on it. */
    public void announcePendingWrite(PendingWrite write) {
        pendingWritesToAnnounce.add(write);
    }
    
    /** Actually does the announcement! Called in its own thread. */
    private void announceNextQueuedPendingWrite() {
        try {
            PendingWrite writeToAnnounce = 
                    Uninterruptibles.takeUninterruptibly(pendingWritesToAnnounce);
            String writeKey = writeToAnnounce.getKey();
            Version writeVersion = writeToAnnounce.getVersion();
            
            // TODO: Efficiency! Group keys, take multiple writes at a time.
            for (String dependentKey : writeToAnnounce.getValue().getTransactionKeys()) {
                AntiEntropyService.Client neighborClient = neighborClients.get(
                        RoutingHash.hashKey(dependentKey, numServersInCluster));
                neighborClient.ackDependentWriteInPending(dependentKey, writeKey,
                        Version.toThrift(writeVersion));
            }
        } catch (TException e) {
            logger.error("Failure while announcing queued pending write: ", e);
        }
    }
    
    private List<AntiEntropyService.Client> createClientsFromAddresses(
            List<ServerAddress> addresses) {
        
        List<AntiEntropyService.Client> clients = Lists.newArrayList(); 
        for (ServerAddress address : addresses) {
            while (true) {
                try {
                    clients.add(ThriftUtil.getAntiEntropyServiceClient(
                            address.getIP(), Config.getAntiEntropyServerPort()));
                    break;
                } catch (Exception e) {
                    logger.error("Exception while bootstrapping connection with cluster server: " +
                                 address);
                    e.printStackTrace();
                }
            }
        }
        return clients;
    }
    
    private static class QueuedWrite {
        public final String key;
        public final ThriftDataItem value;
        public QueuedWrite(String key, ThriftDataItem value) {
            this.key = key;
            this.value = value;
        }
    }
}