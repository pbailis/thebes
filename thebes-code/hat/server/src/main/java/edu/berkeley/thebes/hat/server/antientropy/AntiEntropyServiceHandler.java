package edu.berkeley.thebes.hat.server.antientropy;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import edu.berkeley.thebes.hat.common.thrift.DataDependencyRequest;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceHandler.class);
    
    DependencyResolver dependencyResolver;
    AntiEntropyServiceRouter router;

    public AntiEntropyServiceHandler(AntiEntropyServiceRouter router, DependencyResolver dependencyResolver) {
        this.dependencyResolver = dependencyResolver;
        this.router = router;
        startFulfillRequestThread();
    }

    @Override
    public void put(String key,
                    ThriftDataItem value) throws TException{
    	logger.trace("Received anti-entropy put for key!");
        dependencyResolver.asyncApplyNewWrite(key,
                                              DataItem.fromThrift(value));
    }

    private LinkedBlockingQueue<DataDependencyRequest> requestsToFulfill = new LinkedBlockingQueue<DataDependencyRequest>();

    // todo: make this multi-threaded/not stupid
    private void startFulfillRequestThread() {
        (new Thread(new Runnable() {
            @Override
            public void run() {
                while(true) {
                    try {
                        DataDependencyRequest request = requestsToFulfill.take();
                        logger.debug("Waiting for atomic dependency...");
                        dependencyResolver.blockForAtomicDependency(DataDependency.fromThrift(request.getDependency()));
                        logger.debug("Waiting FULFILLED for atomic dependency...");
                        router.sendDependencyResolved(request);
                    } catch(Exception e) {
                        logger.error("error: ", e);
                    }
                }
            }
        })).start();
    }

    @Override
    public void waitForTransactionalDependencies(List<DataDependencyRequest> requestList) {
       requestsToFulfill.addAll(requestList);
    }

    @Override
    public void receiveTransactionalDependencies(List<Long> fulfilledRequestIds) {
        for(long id : fulfilledRequestIds) {
            router.resolvedDependencyArrived(id);
        }
    }
}