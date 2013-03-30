package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.RoutingMode;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService.AsyncClient.put_call;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService.Client;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService.AsyncClient.get_call;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class QuorumReplicaRouter extends ReplicaRouter {

    private static Logger logger = LoggerFactory.getLogger(QuorumReplicaRouter.class);

    private final Map<Integer, List<ServerAddress>> replicaAddressesByCluster;
    private final int numClusters;
    private final int numNeighbors;
    private final int quorum;

    private final Map<ServerAddress, ReplicaClient> replicaRequestQueues = Maps.newHashMap();

    private class ReplicaClient {
        private Client client;
        private AtomicBoolean inUse;
        BlockingQueue<Request<?>> requestBlockingQueue;

        public ReplicaClient(Client client) {
            this.client = client;
            this.inUse = new AtomicBoolean(false);
            requestBlockingQueue = Queues.newLinkedBlockingQueue();

            new Thread(new Runnable() {
                @Override
                public void run() {
                    while(true) {
                        Request<?> request = Uninterruptibles.takeUninterruptibly(requestBlockingQueue);

                        request.process(ReplicaClient.this);
                    }
                }
            }).start();
        }

        public boolean executeRequest(Request<?> request) {
            if(!inUse.getAndSet(true)) {
                requestBlockingQueue.add(request);
                return true;
            }
            return false;
        }
    }

    public QuorumReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.QUORUM);

        this.replicaAddressesByCluster = Maps.newHashMap();
        this.numClusters = Config.getNumClusters();
        this.numNeighbors = Config.getServersInCluster().size();
        this.quorum = (int) Math.ceil((numNeighbors+1)/2);

        logger.debug("Quorum is set to "+this.quorum);

        for (int i = 0; i < numClusters; i ++) {
            List<ServerAddress> neighbors = Config.getServersInCluster(i+1);
            for (ServerAddress neighbor : neighbors) {
                logger.debug("Connecting to " + neighbor);
                replicaRequestQueues.put(neighbor, new ReplicaClient(
                        ThriftUtil.getReplicaServiceSyncClient(neighbor.getIP(), neighbor.getPort())));
            }
            replicaAddressesByCluster.put(i+1, neighbors);
        }
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        return performRequest(key, new WriteRequest(key, value));
    }

    @Override
    public ThriftDataItem get(String key, Version requiredVersion) throws TException {
        return performRequest(key, new ReadRequest(key, requiredVersion));
    }

    /** Performs the request by queueing N requests and waiting for Q responses. */
    public <E> E performRequest(String key, Request<E> request) {
        int numSent = 0;
        int replicaIndex = RoutingHash.hashKey(key, numNeighbors);
        for (List<ServerAddress> replicasInCluster : replicaAddressesByCluster.values()) {
            ServerAddress replicaAddress = replicasInCluster.get(replicaIndex);
            ReplicaClient replica = replicaRequestQueues.get(replicaAddress);
            if(replica.executeRequest(request))
                numSent++;
        }
        E ret = request.getResponseWhenReady();
        return ret;
    }

    private abstract class Request<E> {
        private AtomicBoolean responseSent;
        private BlockingQueue<E> responseChannel;

        private Request() {
            this.responseSent = new AtomicBoolean(false);
            this.responseChannel = Queues.newLinkedBlockingQueue();
        }

        abstract public void process(ReplicaClient client);

        protected void sendResponse(E response) {
            if (!responseSent.getAndSet(true)) {
                responseChannel.add(response);
            }
        }

        public E getResponseWhenReady() {
            return Uninterruptibles.takeUninterruptibly(responseChannel);
        }
    }

    private class WriteRequest extends Request<Boolean> {
        private String key;
        private ThriftDataItem value;

        private AtomicInteger numAcks = new AtomicInteger(0);
        private AtomicInteger numNacks = new AtomicInteger(0);

        public WriteRequest(String key, DataItem value) {
            this.key = key;
            this.value = value.toThrift();
        }

        public void process(ReplicaClient replica) {
            try {
                replica.client.put(key, value);

                if (numAcks.incrementAndGet() + numNacks.get() >= quorum) {
                    sendResponse(true);
                }
            } catch (TException e) {
                logger.error("Error: ", e);

                if (numNacks.incrementAndGet() + numAcks.get() >= quorum) {
                    sendResponse(false);
                }
            } finally {
                replica.inUse.set(false);
            }
        }
    }

    private class ReadRequest extends Request<ThriftDataItem> {
        private String key;
        private Version requiredVersion;
        private SortedSet<DataItem> returnedDataItems;
        private AtomicInteger numAcks = new AtomicInteger(0);
        private AtomicInteger numNacks = new AtomicInteger(0);


        public ReadRequest(String key, Version requiredVersion) {
            this.key = key;
            this.requiredVersion = requiredVersion != null ? requiredVersion : Version.NULL_VERSION;
            this.returnedDataItems = new ConcurrentSkipListSet<DataItem>();
        }

        public void process(ReplicaClient replica) {
            try {
                ThriftDataItem resp = replica.client.get(key, requiredVersion.getThriftVersion());

                if (resp != null && resp.getVersion() != null) {
                    returnedDataItems.add(new DataItem(resp));
                }

                if (numAcks.incrementAndGet() >= quorum) {
                    if (returnedDataItems.isEmpty()) {
                        sendResponse(new ThriftDataItem()); // "null"
                    } else {
                        sendResponse(returnedDataItems.last().toThrift());
                    }
                }

            } catch (TException e) {
                logger.error("Exception:", e);

                if (numNacks.incrementAndGet() >= quorum) {
                    sendResponse(new ThriftDataItem()); // "null"
                }
            } finally {
                replica.inUse.set(false);
            }
        }
    }
}
