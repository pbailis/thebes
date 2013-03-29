package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MasteredReplicaRouter extends ReplicaRouter {

    private final Map<Integer, List<ServerAddress>> replicaAddressesByCluster;
    private final Map<Integer, List<ReplicaService.Client>> syncReplicasByCluster;
    private final int numClusters;
    private final int numNeighbors;

    public MasteredReplicaRouter() throws TTransportException, IOException {
        assert(Config.shouldRouteToMasters());
        
        this.replicaAddressesByCluster = Maps.newHashMap();
        this.syncReplicasByCluster = Maps.newHashMap();
        this.numClusters = Config.getSiblingServers().size();
        this.numNeighbors = Config.getServersInCluster().size();
        
        for (int i = 0; i < numClusters; i ++) {
            List<ServerAddress> neighbors = Config.getServersInCluster();
            List<ReplicaService.Client> neighborClients = Lists.newArrayList();
            for (ServerAddress neighbor : neighbors) {
                neighborClients.add(ThriftUtil.getReplicaServiceSyncClient(neighbor.getIP(),
                        neighbor.getPort()));
            }
            replicaAddressesByCluster.put(i+1, neighbors);
            syncReplicasByCluster.put(i+1, neighborClients);
        }
    }

    @Override
    public ReplicaService.Client getSyncReplicaByKey(String key) {
        int hash = RoutingHash.hashKey(key, numNeighbors);
        int clusterID = (hash % numClusters) + 1;
        return syncReplicasByCluster.get(clusterID).get(hash);
    }

    @Override
    public ReplicaService.AsyncClient getAsyncReplicaByKey(String key) {
        throw new UnsupportedOperationException("Unsupported! (Needs to be updated if you want to use.)");
//        return asyncReplicas.get(RoutingHash.hashKey(key, asyncReplicas.size()));
    }

    @Override
    public ServerAddress getReplicaIPByKey(String key) {
        int hash = RoutingHash.hashKey(key, numNeighbors);
        int clusterID = (hash % numClusters) + 1;
        return replicaAddressesByCluster.get(clusterID).get(hash);
    }
}
