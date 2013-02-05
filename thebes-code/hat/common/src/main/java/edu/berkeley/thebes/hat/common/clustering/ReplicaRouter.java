package edu.berkeley.thebes.hat.common.clustering;

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.transport.TTransportException;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;

public class ReplicaRouter {
    private static List<ReplicaService.Client> replicas;

    public ReplicaRouter() throws TTransportException {
        List<ServerAddress> serverIPs = Config.getServersInCluster();
        replicas = new ArrayList<ReplicaService.Client>(serverIPs.size());

        for(ServerAddress server : serverIPs)
            replicas.add(ThriftUtil.getReplicaServiceClient(
                    server.getIP(), server.getPort()));
    }

    public ReplicaService.Client getReplicaByKey(String key) {
        return replicas.get(key.hashCode() % replicas.size());
    }
}