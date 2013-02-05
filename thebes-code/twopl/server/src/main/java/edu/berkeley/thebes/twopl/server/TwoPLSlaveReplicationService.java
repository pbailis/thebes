package edu.berkeley.thebes.twopl.server;

import com.google.common.collect.Lists;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLSlaveReplicaService;
import edu.berkeley.thebes.twopl.common.thrift.TwoPLThriftUtil;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

public class TwoPLSlaveReplicationService {
    private static Logger logger = LoggerFactory.getLogger(TwoPLSlaveReplicationService.class);

    private List<TwoPLSlaveReplicaService.Client> slaveReplicas;
    
    public TwoPLSlaveReplicationService() {
        this.slaveReplicas = Collections.emptyList();
    }

    public void connectSlaves() {
        slaveReplicas = Lists.newArrayList();

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
        }

        logger.debug("Bootstrapping slave replication service...");

        for (String slave : Config.getSiblingServers()) {
            while (true) {
                try {
                    slaveReplicas.add(
                            TwoPLThriftUtil.getSlaveReplicaServiceClient(slave,
                                    Config.getTwoPLServerPort()));
                    break;
                } catch (TTransportException e) {
                    System.err.println("Exception while bootstrapping connection with slave: " +
                                       slave + ":" + Config.getTwoPLServerPort());
                    e.printStackTrace();
                }
            }
        }

        logger.debug("...slave replication service bootstrapped");
    }

    // TODO: race condition between serving and when we've connected to neighbors
    public void sendToSlaves(String key, DataItem value) throws TException {
        for (TwoPLSlaveReplicaService.Client slave : slaveReplicas) {
            slave.send_put(key, value);
        }
    }
}
