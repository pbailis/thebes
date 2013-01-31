package edu.berkeley.thebes.hat.server;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftUtil;
import edu.berkeley.thebes.hat.server.replica.AntiEntropyServiceHandler;

import org.apache.thrift.TException;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class AntiEntropyServer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServer.class);

    private List<AntiEntropyService.Client> neighborClients;
    private AntiEntropyServiceHandler serviceHandler;
    
    public AntiEntropyServer(AntiEntropyServiceHandler serviceHandler) {
        this.neighborClients = Collections.emptyList();
        this.serviceHandler = serviceHandler;
    }

    public void run() {
        try {
            AntiEntropyService.Processor<AntiEntropyServiceHandler> processor =
                    new AntiEntropyService.Processor<AntiEntropyServiceHandler>(serviceHandler);
            
            TServerTransport serverTransport = new TServerSocket(Config.getAntiEntropyServerBindIP());
            TServer server = new TThreadPoolServer(
                    new TThreadPoolServer.Args(serverTransport).processor(processor));
    
            logger.debug("Starting the anti-entropy server...");
    
            server.serve();
        } catch (TTransportException e) {
            throw new RuntimeException(e);
        }
    }

    public void connectNeighbors() {
        neighborClients = new ArrayList<AntiEntropyService.Client>();

        try {
            Thread.sleep(5000);
        } catch (Exception e) {
        }

        logger.debug("Bootstrapping anti-entropy...");

        for (String neighbor : Config.getNeighborServers()) {
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