package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.TException;
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
import edu.berkeley.thebes.hat.common.thrift.ReplicaService.Client;
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
    private List<ServerAddress> allReplicaAddresses;
    private final int numClusters;
    private final int numNeighbors;
    private final Random randomGenerator;
    private int quorum;
    
    private final Map<ServerAddress, ReplicaRequestQueue> replicaRequestQueues = Maps.newHashMap();

    private class ReplicaRequestQueue {
        private ReplicaService.Client client;
        private BlockingQueue<Request<?>> queue;
        private Lock lock;

        public ReplicaRequestQueue(Client client, BlockingQueue<Request<?>> queue, Lock lock) {
            this.client = client;
            this.queue = queue;
            this.lock = lock;
        }
    }

    public QuorumReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.QUORUM);
        
        this.replicaAddressesByCluster = Maps.newHashMap();
        this.numClusters = Config.getNumClusters();
        this.numNeighbors = Config.getServersInCluster().size();
        this.quorum = (int) Math.ceil((numNeighbors+1)/2);
        this.randomGenerator = new Random();
        this.allReplicaAddresses = Lists.newArrayList();
        
        for (int i = 0; i < numClusters; i ++) {
            List<ServerAddress> neighbors = Config.getServersInCluster(i+1);
            for (ServerAddress neighbor : neighbors) {
                replicaRequestQueues.put(neighbor, new ReplicaRequestQueue(
                        ThriftUtil.getReplicaServiceSyncClient(neighbor.getIP(), neighbor.getPort()),
                        Queues.<Request<?>>newLinkedBlockingQueue(), new ReentrantLock()));
                allReplicaAddresses.add(neighbor);
            }
            replicaAddressesByCluster.put(i+1, neighbors);
        }
        
        for (int i = 0; i < Config.getNumQuorumThreads(); i ++) {
            new Thread() {
                @Override
                public void run() {
                    while (true) {
                        boolean foundAnyRequests = executePendingRequest();
                        if (!foundAnyRequests) {
                            Uninterruptibles.sleepUninterruptibly(100, TimeUnit.MILLISECONDS);
                        }
                    }
                }
            }.start();
            
            // Stagger thread creation times so they sweep at different intervals
            Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MILLISECONDS);
        }
    }
    
    /** Finds an executes some pending request. Returns true if we found at least one to execute. */
    @SuppressWarnings("rawtypes")
    public boolean executePendingRequest() {
        int start = randomGenerator.nextInt(allReplicaAddresses.size());
        int i = start;
        do {
            ServerAddress address = allReplicaAddresses.get(i);
            ReplicaRequestQueue requestQueue = replicaRequestQueues.get(address);
            if (requestQueue.lock.tryLock()) {
                try {
                    // We own the lock, meaning we're the only ones using this replica!
                    if (!requestQueue.queue.isEmpty()) {
                        // And someone wants to write to this replica...
                        List<Request> allPendingRequests = Lists.newArrayList();
                        requestQueue.queue.drainTo(allPendingRequests);
                        
                        for (Request r : allPendingRequests) {
                            r.process(requestQueue.client);
                        }
                        
                        return true;
                    }
                } finally {
                    requestQueue.lock.unlock();
                }
            }
            
            i = (i+1) % allReplicaAddresses.size();
        } while (i != start);
        
        return false;
    }

    
    @Override
    public boolean put(String key, DataItem value) throws TException {
        return performRequest(key, new WriteProcessor(key, value));
    }

    @Override
    public ThriftDataItem get(String key, Version requiredVersion) throws TException {
        return performRequest(key, new ReadProcessor(key, requiredVersion));
    }
    
    /** Performs the request by queueing N requests and waiting for Q responses. */
    public <E> E performRequest(String key, Processor<E> processor) {
        Request<E> request = Request.forProcessor(processor);
        int replicaIndex = RoutingHash.hashKey(key, numNeighbors);
        for (List<ServerAddress> replicasInCluster : replicaAddressesByCluster.values()) {
            ServerAddress replicaAddress = replicasInCluster.get(replicaIndex);
            replicaRequestQueues.get(replicaAddress).queue.add(request);
        }
        return request.getResponseWhenReady();
    }
    
    private static class Request<E> {
        private Processor<E> processor;
        private AtomicBoolean responseSent;
        private BlockingQueue<E> responseChannel;
        
        private Request(Processor<E> processor) {
            this.processor = processor;
            this.responseSent = new AtomicBoolean(false);
            this.responseChannel = Queues.newLinkedBlockingQueue();
        }

        public static <E> Request<E> forProcessor(Processor<E> processor) {
            return new Request<E>(processor);
        }
        
        public void process(ReplicaService.Client client) {
            if (!responseSent.get()) {
                E response = processor.process(client);
                if (response != null) {
                    sendResponse(response);
                }
            }
        }
        
        private void sendResponse(E response) {
            if (!responseSent.getAndSet(true)) {
                responseChannel.add(response);
            }
        }
        
        /** Returns the response after waiting for Q responses from our N replicas. */
        public E getResponseWhenReady() {
            return Uninterruptibles.takeUninterruptibly(responseChannel);
        }
    }
    
    /** A Processor is in charge of individual client communications + aggregating the results. */
    public interface Processor<E> {
        /** Return null if we're not ready to return a result, or else the result of the quorum. */
        public E process(ReplicaService.Client client);
    }
    
    private class WriteProcessor implements Processor<Boolean> {
        private String key;
        private ThriftDataItem value;
        
        private AtomicInteger numAcks = new AtomicInteger(0);
        private AtomicInteger numNacks = new AtomicInteger(0);

        public WriteProcessor(String key, DataItem value) {
            this.key = key;
            this.value = value.toThrift();
        }
        
        public Boolean process(ReplicaService.Client client) {
            try {
                client.put(key, value);
                numAcks.incrementAndGet();
            } catch (TException e) {
                logger.error("Bad stuff happened: ", e);
                numNacks.incrementAndGet();
            }
            
            if (numAcks.get() > quorum) {
                return true;
            } else if (numNacks.get() > quorum) {
                return false;
            }
            return null;
        }
    }

    private class ReadProcessor implements Processor<ThriftDataItem> {
        private String key;
        private Version requiredVersion;
        private SortedSet<DataItem> returnedDataItems;
        private AtomicInteger numAcks = new AtomicInteger(0);
        private AtomicInteger numNacks = new AtomicInteger(0);
        

        public ReadProcessor(String key, Version requiredVersion) {
            this.key = key;
            this.requiredVersion = requiredVersion;
            this.returnedDataItems = new ConcurrentSkipListSet<DataItem>();
        }

        public ThriftDataItem process(ReplicaService.Client client) {
            try {
                ThriftDataItem resp = client.get(key, Version.toThrift(requiredVersion));
                if (resp != null && resp.getVersion() != null) {
                    returnedDataItems.add(new DataItem(resp));
                }
                numAcks.incrementAndGet();
            } catch (TException e) {
                logger.error("Bad stuff happened: ", e);
                numNacks.incrementAndGet();
            }
            
            if (numAcks.get() > quorum) {
                if (returnedDataItems.isEmpty()) {
                    return new ThriftDataItem(); // "null"
                } else {
                    return returnedDataItems.first().toThrift();
                }
            } else if (numNacks.get() > quorum) {
                return new ThriftDataItem(); // "null"
            }
            
            return null;
        }
    }
}
