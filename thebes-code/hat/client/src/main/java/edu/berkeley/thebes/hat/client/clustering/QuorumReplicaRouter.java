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
    private int quorum;
    
    private final Map<ServerAddress, ReplicaClient> replicaRequestQueues = Maps.newHashMap();

    private class ReplicaClient {
        private ReplicaService.AsyncClient client;
        private AtomicBoolean inUse;

        public ReplicaClient(ReplicaService.AsyncClient client) {
            this.client = client;
            this.inUse = new AtomicBoolean(false);
        }
    }

    public QuorumReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.QUORUM);
        
        this.replicaAddressesByCluster = Maps.newHashMap();
        this.numClusters = Config.getNumClusters();
        this.numNeighbors = Config.getServersInCluster().size();
        this.quorum = (int) Math.ceil((numNeighbors+1)/2);
        
        for (int i = 0; i < numClusters; i ++) {
            List<ServerAddress> neighbors = Config.getServersInCluster(i+1);
            for (ServerAddress neighbor : neighbors) {
                logger.error("Connecting to " + neighbor);
                replicaRequestQueues.put(neighbor, new ReplicaClient(
                        ThriftUtil.getReplicaServiceAsyncClient(neighbor.getIP(), neighbor.getPort())));
            }
            replicaAddressesByCluster.put(i+1, neighbors);
        }
    }
    
    @Override
    public boolean put(String key, DataItem value) throws TException {
        logger.error("PUT for key " + key);
        return performRequest(key, new WriteRequest(key, value));
    }

    @Override
    public ThriftDataItem get(String key, Version requiredVersion) throws TException {
        logger.error("GET for key " + key);
        return performRequest(key, new ReadRequest(key, requiredVersion));
    }
    
    /** Performs the request by queueing N requests and waiting for Q responses. */
    public <E> E performRequest(String key, Request<E> request) {
        int numSent = 0;
        int replicaIndex = RoutingHash.hashKey(key, numNeighbors);
        for (List<ServerAddress> replicasInCluster : replicaAddressesByCluster.values()) {
            ServerAddress replicaAddress = replicasInCluster.get(replicaIndex);
            ReplicaClient replica = replicaRequestQueues.get(replicaAddress);
            if (!replica.inUse.getAndSet(true)) {
                numSent ++;
                request.process(replica);
            }
        }
        logger.error("Sent " + numSent + " messages for key " + key);
        E ret = request.getResponseWhenReady();
        logger.error("Received response " + ret + " for key " + key);
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
            PutCallback callback = new PutCallback(replica);
            try {
                logger.error("Calling put...");
                replica.client.put(key, value, callback);
                logger.error(replica.client.hasTimeout() + " / " + replica.client.getTimeout());
                logger.error("Called put...");
            } catch (TException e) {
                logger.error("Exception happened!");
                callback.onError(e);
            }
        }
        
        private class PutCallback implements AsyncMethodCallback<put_call> {
            ReplicaClient replica;
            public PutCallback(ReplicaClient replica) {
                this.replica = replica;
            }
            
            @Override
            public void onComplete(put_call response) {
                logger.error("Response for " + key + "!");
                if (numAcks.incrementAndGet() > quorum) {
                    sendResponse(true);
                }
                replica.inUse.set(false);
            }
    
            @Override
            public void onError(Exception exception) {
                logger.error("Bad stuff happened: ", exception);
                numNacks.incrementAndGet();
                if (numNacks.incrementAndGet() > quorum) {
                    sendResponse(false);
                }
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
            this.requiredVersion = requiredVersion;
            this.returnedDataItems = new ConcurrentSkipListSet<DataItem>();
        }

        public void process(ReplicaClient replica) {
            AsyncMethodCallback<get_call> callback = new GetCallback(replica);
            try {
                replica.client.get(key, Version.toThrift(requiredVersion), callback);
            } catch (TException e) {
                callback.onError(e);
            }
        }
        
        private class GetCallback implements AsyncMethodCallback<get_call> {
            ReplicaClient replica;
            public GetCallback(ReplicaClient replica) {
                this.replica = replica;
            }
            
            @Override
            public void onComplete(get_call response) {
                ThriftDataItem resp;
                try {
                    resp = response.getResult();
                } catch (TException e) {
                    onError(e);
                    return;
                }
                
                if (resp != null && resp.getVersion() != null) {
                    returnedDataItems.add(new DataItem(resp));
                }
                
                if (numAcks.incrementAndGet() > quorum) {
                    if (returnedDataItems.isEmpty()) {
                        sendResponse(new ThriftDataItem()); // "null"
                    } else {
                        sendResponse(returnedDataItems.last().toThrift());
                    }
                }
                replica.inUse.set(false);
            }
    
            @Override
            public void onError(Exception exception) {
                logger.error("Bad stuff happened: ", exception);
                numNacks.incrementAndGet();
                if (numNacks.incrementAndGet() > quorum) {
                    sendResponse(new ThriftDataItem()); // "null"
                }
                replica.inUse.set(false);
            }
        }
    }
}
