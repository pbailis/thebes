package edu.berkeley.thebes.twopl.common;

import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransportException;

import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.clustering.RoutingHash;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;

import java.util.ArrayList;
import java.util.List;

/** Helps route traffic to the master of each replica set. */
public class TwoPLMasterRouter {
    /** Contains the ordered list of master replicas, one per set of replicas. */
//    private List<TwoPLMasterReplicaService.Client> masterReplicas;
    private List<ServerAddress> masterReplicas;

    public TwoPLMasterRouter() throws TTransportException {
        List<ServerAddress> serverIPs = Config.getMasterServers();
        masterReplicas = Lists.newArrayList(serverIPs);

        /*
        for (ServerAddress server : serverIPs) {
            masterReplicas.add(TwoPLThriftUtil.getMasterReplicaServiceClient(
                    server.getIP(), server.getPort()));
        }
        */
    }

    public TwoPLMasterReplicaService.Client getMasterByKey(String key) throws TTransportException {
        int index = RoutingHash.hashKey(key, masterReplicas.size());
        return TwoPLThriftUtil.getMasterReplicaServiceClient(
                masterReplicas.get(index).getIP(),
                masterReplicas.get(index).getPort());
    }
    
    public ServerAddress getMasterAddressByKey(String key) {
        List<ServerAddress> servers = Config.getMasterServers();
        return servers.get(RoutingHash.hashKey(key, servers.size()));
    }
}