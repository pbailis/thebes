package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.transport.TTransportException;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class StandardReplicaRouter extends ReplicaRouter {

    private List<ReplicaService.Client> syncReplicas;
    private List<ReplicaService.AsyncClient> asyncReplicas;

    public StandardReplicaRouter() throws TTransportException, IOException {
        assert(!Config.shouldRouteToMasters());
        
        List<ServerAddress> serverIPs = Config.getServersInCluster();
        
        syncReplicas = new ArrayList<ReplicaService.Client>(serverIPs.size());
        asyncReplicas = new ArrayList<ReplicaService.AsyncClient>(serverIPs.size());

        for (ServerAddress server : serverIPs) {
            syncReplicas.add(ThriftUtil.getReplicaServiceSyncClient(server.getIP(), server.getPort()));
            asyncReplicas.add(ThriftUtil.getReplicaServiceAsyncClient(server.getIP(), server.getPort()));
        }
    }

    @Override
    public ReplicaService.Client getSyncReplicaByKey(String key) {
        return syncReplicas.get(RoutingHash.hashKey(key, syncReplicas.size()));
    }

    @Override
    public ReplicaService.AsyncClient getAsyncReplicaByKey(String key) {
        return asyncReplicas.get(RoutingHash.hashKey(key, asyncReplicas.size()));
    }

    @Override
    public ServerAddress getReplicaIPByKey(String key) {
        return Config.getServersInCluster().get(RoutingHash.hashKey(key, syncReplicas.size()));
    }
}
