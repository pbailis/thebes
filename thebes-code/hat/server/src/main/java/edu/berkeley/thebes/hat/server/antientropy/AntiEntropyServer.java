package edu.berkeley.thebes.hat.server.antientropy;

import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftServer;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftDataDependency;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;


public class AntiEntropyServer implements Runnable {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServer.class);

    AntiEntropyServiceRouter router;
    private AntiEntropyServiceHandler serviceHandler;

    public AntiEntropyServer(AntiEntropyServiceHandler serviceHandler) throws TTransportException {
        router = new AntiEntropyServiceRouter();
        this.serviceHandler = serviceHandler;
    }

    public void run() {
        logger.debug("Starting the anti-entropy server...");
        ThriftServer.startInCurrentThread(
                new AntiEntropyService.Processor<AntiEntropyServiceHandler>(serviceHandler),
                Config.getAntiEntropyServerBindIP());
    }

    //todo: change interface
    //todo: race condition between serving and when we've connected to neighbors
    public void sendToNeighbors(String key,
                                ThriftDataItem value,
                                List<ThriftDataDependency> happensAfter,
                                List<String> transactionKeys) throws TException {
        for (AntiEntropyService.Client neighbor : router.getNeighborClients()) {
            logger.debug("sending to neighbor");
            neighbor.send_put(key, value, happensAfter, transactionKeys);
            logger.debug("sent to neighbor");
        }
    }
}