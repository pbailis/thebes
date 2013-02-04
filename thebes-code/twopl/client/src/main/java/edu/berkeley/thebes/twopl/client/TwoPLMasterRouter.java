package edu.berkeley.thebes.twopl.client;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLMasterReplicaService;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;

import org.apache.thrift.transport.TTransportException;

import java.util.ArrayList;
import java.util.List;

/** Helps route traffic to the master of each replica set. */
public class TwoPLMasterRouter {
    /** Contains the ordered list of master replicas, one per set of replicas. */
    private static List<TwoPLMasterReplicaService.Client> masterReplicas;

    public TwoPLMasterRouter() throws TTransportException {
        List<String> serverIPs = Config.getMasterServers();
        masterReplicas = new ArrayList<TwoPLMasterReplicaService.Client>(serverIPs.size());

        for (String server : serverIPs) {
            masterReplicas.add(TwoPLThriftUtil.getMasterReplicaServiceClient(
                    server,  Config.getTwoPLServerPort()));
        }
    }

    public TwoPLMasterReplicaService.Client getMasterByKey(String key) {
        return masterReplicas.get(key.hashCode() % masterReplicas.size());
    }
}