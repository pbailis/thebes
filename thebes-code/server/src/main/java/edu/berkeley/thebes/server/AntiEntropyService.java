package edu.berkeley.thebes.server;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.common.thrift.ThriftUtil;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AntiEntropyService implements Runnable {
    private List<ReplicaService.Client> neighborClients;
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyService.class);

    public void run() {
        neighborClients = new ArrayList<ReplicaService.Client>();

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
        }

        logger.debug("Bootstrapping anti-entropy...");

        for (String neighbor : Config.getNeighborServers()) {
            while (true) {
                try {
                    neighborClients.add(ThriftUtil.getReplicaServiceClient(neighbor, Config.getServerPort()));
                    break;
                } catch (TTransportException e) {
                    System.err.println("Exception while bootstrapping connection with neighbor: " +
                                       neighbor + ":" + Config.getServerPort());
                    e.printStackTrace();
                }
            }
        }

        logger.debug("...anti-entropy bootstrapped");
    }


    //todo: change interface
    //todo: race condition between serving and when we've connected to neighbors
    public void sendToNeighbors(String key, ByteBuffer value) throws TException {
        for (ReplicaService.Client neighbor : neighborClients) {
            logger.debug("sending to neighbor");
            neighbor.send_put(key, value);
            logger.debug("sent to neighbor");

        }
    }
}