package edu.berkeley.thebes.hat.client.clustering;

import org.apache.thrift.transport.TTransportException;

import java.io.IOException;

import edu.berkeley.thebes.common.thrift.ServerAddress;
import edu.berkeley.thebes.hat.common.thrift.ReplicaService;

public abstract class ReplicaRouter {
    public static ReplicaRouter newInstance(boolean shouldRouteToMasters)
            throws IOException, TTransportException {
        if (shouldRouteToMasters) {
            return new MasteredReplicaRouter();
        } else {
            return new StandardReplicaRouter();
        }
    }
    
    abstract public ReplicaService.Client getSyncReplicaByKey(String key);
    abstract public ReplicaService.AsyncClient getAsyncReplicaByKey(String key);
    abstract public ServerAddress getReplicaIPByKey(String key);
}