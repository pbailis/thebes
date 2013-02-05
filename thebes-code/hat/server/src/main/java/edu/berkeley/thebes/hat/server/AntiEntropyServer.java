package edu.berkeley.thebes.hat.server;

import java.util.Collections;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftUtil;
import edu.berkeley.thebes.hat.server.replica.AntiEntropyServiceHandler;

public class AntiEntropyServer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServer.class);

    private List<AntiEntropyService.Client> neighborClients;
    private AntiEntropyServiceHandler serviceHandler;
    
    public AntiEntropyServer(AntiEntropyServiceHandler serviceHandler) {
        this.neighborClients = Collections.emptyList();
        this.serviceHandler = serviceHandler;
    }

    public void run() {
        logger.debug("Starting the anti-entropy server...");
        ThriftServer.startInCurrentThread(
                new AntiEntropyService.Processor<AntiEntropyServiceHandler>(serviceHandler),
                Config.getAntiEntropyServerBindIP());
    }

    public void connectNeighbors() {
        neighborClients = Lists.newArrayList();

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
        }

        logger.debug("Bootstrapping anti-entropy...");

        for (String neighbor : Config.getSiblingServers()) {
            while (true) {
                try {
                    neighborClients.add(
                            ThriftUtil.getAntiEntropyServiceClient(neighbor,
                                                                   Config.getAntiEntropyServerPort()));
                    break;
                } catch (TTransportException e) {
                    System.err.println("Exception while bootstrapping connection with neighbor: " +
                                       neighbor + ":" + Config.getAntiEntropyServerPort());
                    e.printStackTrace();
                }
            }
        }

        logger.debug("...anti-entropy bootstrapped");
    }


    //todo: change interface
    //todo: race condition between serving and when we've connected to neighbors
    public void sendToNeighbors(String key, DataItem value) throws TException {
        for (AntiEntropyService.Client neighbor : neighborClients) {
            logger.debug("sending to neighbor");
            neighbor.send_put(key, value);
            logger.debug("sent to neighbor");

        }
    }
}