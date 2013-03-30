package edu.berkeley.thebes.hat.client.clustering;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.common.util.concurrent.Uninterruptibles;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.RoutingMode;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class NearestReplicaRouter extends ReplicaRouter {

    private static Logger logger = LoggerFactory.getLogger(NearestReplicaRouter.class);


    private class ReplicaClient {
        private ReplicaService.Client client;
        BlockingQueue<Request<?>> requestBlockingQueue;

        public ReplicaClient(ReplicaService.Client client) {
            this.client = client;
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

        public void executeRequest(Request<?> request) {
            requestBlockingQueue.add(request);
        }
    }

    private List<ReplicaClient> syncReplicas;

    public NearestReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.NEAREST);
        
        List<ServerAddress> serverIPs = Config.getServersInCluster();
        
        syncReplicas = new ArrayList<ReplicaClient>(serverIPs.size());

        for (ServerAddress server : serverIPs) {
            syncReplicas.add(new ReplicaClient(ThriftUtil.getReplicaServiceSyncClient(server.getIP(), server.getPort())));
        }
    }
    
    private ReplicaClient getSyncReplicaByKey(String key) {
        return syncReplicas.get(RoutingHash.hashKey(key, syncReplicas.size()));
    }

    private ServerAddress getReplicaIPByKey(String key) {
        return Config.getServersInCluster().get(RoutingHash.hashKey(key, syncReplicas.size()));
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        try {
            return getSyncReplicaByKey(key).client.put(key, value.toThrift());
        } catch (TException e) {
            throw new TException("Failed to write to " + getReplicaIPByKey(key), e);
        }
    }

    @Override
    public boolean put_all(Map<String, DataItem> pairs) throws TException {
        Map<ReplicaClient, Map<String,ThriftDataItem>> pendingWrites = Maps.newHashMap();
        for(String key : pairs.keySet()) {
            ReplicaClient client = getSyncReplicaByKey(key);

            if(!pendingWrites.containsKey(client)) {
                pendingWrites.put(client, new HashMap<String, ThriftDataItem>());
            }

            pendingWrites.get(client).put(key, pairs.get(key).toThrift());
        }

        Semaphore notifyDone = new Semaphore(0);
        for(ReplicaClient client : pendingWrites.keySet()) {
            client.executeRequest(new PutAllRequest(pendingWrites.get(client), notifyDone));
        }

        for(int i = 0; i < pendingWrites.keySet().size(); ++i) {
            notifyDone.acquireUninterruptibly();
        }

        return true;
    }

    @Override
    public ThriftDataItem get(String key, Version requiredVersion) throws TException {
        try {
            return getSyncReplicaByKey(key).client.get(key, Version.toThrift(requiredVersion));
        } catch (TException e) {
            throw new TException("Failed to read from " + getReplicaIPByKey(key), e);
        }
    }

    @Override
    public Map<String, ThriftDataItem> get_all(Map<String, Version> keys) throws TException {
        Map<ReplicaClient, Map<String,ThriftVersion>> toFetch = Maps.newHashMap();
        for(String key : keys.keySet()) {
            ReplicaClient client = getSyncReplicaByKey(key);

            if(!toFetch.containsKey(client)) {
                toFetch.put(client, new HashMap<String, ThriftVersion>());
            }

            toFetch.get(client).put(key, keys.get(key).getThriftVersion());
        }

        Semaphore notifyDone = new Semaphore(0);
        Map<String, ThriftDataItem> results = Maps.newConcurrentMap();
        for(ReplicaClient client : toFetch.keySet()) {
            client.executeRequest(new GetAllRequest(toFetch.get(client), results, notifyDone));
        }

        for(int i = 0; i < toFetch.keySet().size(); ++i) {
            notifyDone.acquireUninterruptibly();
        }

        return results;
    }

    private abstract class Request<E> {
        abstract public void process(ReplicaClient client);
    }

    private class PutAllRequest extends Request<Boolean> {
        private Map<String, ThriftDataItem> pairs;
        private Semaphore doneSemaphore;

        public PutAllRequest(Map<String, ThriftDataItem> pairs, Semaphore doneSemaphore) {
            this.pairs = pairs;
            this.doneSemaphore = doneSemaphore;
        }

        public void process(ReplicaClient replica) {
            try {
                replica.client.put_all(pairs);
            } catch (TException e) {
                logger.warn("Exception:", e);
            } finally {
                doneSemaphore.release();
            }
        }
    }

    private class GetAllRequest extends Request<Boolean> {
        private Map<String, ThriftVersion> keys;
        private Map<String, ThriftDataItem> returnedDataItems;
        Semaphore doneSemaphore;

        public GetAllRequest(Map<String, ThriftVersion> keys, Map<String, ThriftDataItem> returnedDataItems, Semaphore doneSemaphore) {
            this.keys = keys;
            this.returnedDataItems = returnedDataItems;
            this.doneSemaphore = doneSemaphore;
        }

        public void process(ReplicaClient replica) {
            try {
                Map<String, ThriftDataItem> resp = replica.client.get_all(keys);

                if (resp != null) {
                    for(String key : resp.keySet()) {
                        returnedDataItems.put(key, resp.get(key));
                    }
                }
            } catch (TException e) {
                logger.error("Exception happened!", e);
            } finally {
                doneSemaphore.release();
            }
        }
    }


}
