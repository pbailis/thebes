package edu.berkeley.thebes.hat.server.antientropy;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import com.google.common.collect.Lists;
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

    LinkedBlockingQueue<QueuedAntiEntropyWrite> queuedAntiEntropyWrites;

    private class QueuedAntiEntropyWrite {
        private String key;
        private ThriftDataItem value;

        public QueuedAntiEntropyWrite(String key, ThriftDataItem value) {
            this.key = key;
            this.value = value;
        }

        public String getKey() {
            return key;
        }

        public ThriftDataItem getValue() {
            return value;
        }
    }

    private class AntiEntropyRunner implements Runnable {
        LinkedBlockingQueue<QueuedAntiEntropyWrite> writeQueue;

        public AntiEntropyRunner(LinkedBlockingQueue<QueuedAntiEntropyWrite> writeQueue) {
            this.writeQueue = writeQueue;
        }

        public void run() {
            while(true) {
                try {
                    QueuedAntiEntropyWrite queuedAntiEntropyWrite = writeQueue.take();


                    for (AntiEntropyService.Client neighbor : router.getNeighborClients()) {
                        logger.trace("sending to neighbor");
                        try {
                            neighbor.put(queuedAntiEntropyWrite.getKey(),
                                         queuedAntiEntropyWrite.getValue());
                        } catch (RuntimeException e) {
                            logger.debug("errored: " + e);
                            e.printStackTrace();
                        } catch (TException e) {
                                logger.debug("errored: " + e);
                                e.printStackTrace();
                                synchronized (writeQueue) { writeQueue.add(queuedAntiEntropyWrite); }
                        }
                        logger.trace("sent to neighbor");
                    }

                } catch(InterruptedException e) {
                                    logger.warn(e.getMessage());
                                    continue;
                }
            }
        }
    }

    public AntiEntropyServer(AntiEntropyServiceHandler serviceHandler) throws TTransportException {
        router = new AntiEntropyServiceRouter();
        queuedAntiEntropyWrites = new LinkedBlockingQueue<QueuedAntiEntropyWrite>();
        this.serviceHandler = serviceHandler;
    }

    public void run() {
        logger.debug("Starting the anti-entropy server on IP..."+Config.getAntiEntropyServerBindIP());
        (new Thread(new AntiEntropyRunner(queuedAntiEntropyWrites))).start();
        ThriftServer.startInCurrentThread(
                new AntiEntropyService.Processor<AntiEntropyServiceHandler>(serviceHandler),
                Config.getAntiEntropyServerBindIP());
    }

    //todo: change interface
    //todo: race condition between serving and when we've connected to neighbors
    public void sendToNeighbors(String key,
                                ThriftDataItem value) {
        queuedAntiEntropyWrites.add(new QueuedAntiEntropyWrite(key, value));
    }
}