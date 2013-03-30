package edu.berkeley.thebes.hat.client.clustering;

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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class NearestReplicaRouter extends ReplicaRouter {

    private List<ReplicaService.Client> syncReplicas;
    private List<ReplicaService.AsyncClient> asyncReplicas;

    public NearestReplicaRouter() throws TTransportException, IOException {
        assert(Config.getRoutingMode() == RoutingMode.NEAREST);
        
        List<ServerAddress> serverIPs = Config.getServersInCluster();
        
        syncReplicas = new ArrayList<ReplicaService.Client>(serverIPs.size());
        asyncReplicas = new ArrayList<ReplicaService.AsyncClient>(serverIPs.size());

        for (ServerAddress server : serverIPs) {
            syncReplicas.add(ThriftUtil.getReplicaServiceSyncClient(server.getIP(), server.getPort()));
            asyncReplicas.add(ThriftUtil.getReplicaServiceAsyncClient(server.getIP(), server.getPort()));
        }
    }
    
    private ReplicaService.Client getSyncReplicaByKey(String key) {
        return syncReplicas.get(RoutingHash.hashKey(key, syncReplicas.size()));
    }

    private ServerAddress getReplicaIPByKey(String key) {
        return Config.getServersInCluster().get(RoutingHash.hashKey(key, syncReplicas.size()));
    }

    @Override
    public boolean put(String key, DataItem value) throws TException {
        try {
            return getSyncReplicaByKey(key).put(key, value.toThrift());
        } catch (TException e) {
            throw new TException("Failed to write to " + getReplicaIPByKey(key), e);
        }
    }

    @Override
    public ThriftDataItem get(String key, Version requiredVersion) throws TException {
        try {
            return getSyncReplicaByKey(key).get(key, Version.toThrift(requiredVersion));
        } catch (TException e) {
            throw new TException("Failed to read from " + getReplicaIPByKey(key), e);
        }
    }
}
