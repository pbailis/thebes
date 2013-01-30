package edu.berkeley.thebes.common;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.common.thrift.ThriftUtil;
import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.List;

public class ReplicaRouter {
    private static List<ReplicaService.Client> replicas;

    public ReplicaRouter() throws TTransportException {
        List<String> serverIPs = Config.getServersInCluster();
        replicas = new ArrayList<ReplicaService.Client>(serverIPs.size());

        for(String server :serverIPs)
            replicas.add(ThriftUtil.getReplicaServiceClient(server,  Config.getServerPort()));
    }

    public ReplicaService.Client getReplicaByKey(String key) {
        return replicas.get(key.hashCode() % replicas.size());
    }
}