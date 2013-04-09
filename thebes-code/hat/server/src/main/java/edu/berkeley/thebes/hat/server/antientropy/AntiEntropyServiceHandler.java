package edu.berkeley.thebes.hat.server.antientropy;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import edu.berkeley.thebes.hat.common.thrift.DataDependencyRequest;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.data.Version;
import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.common.thrift.ThriftVersion;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceHandler.class);
    
    DependencyResolver dependencyResolver;
    AntiEntropyServiceRouter router;
    IPersistenceEngine persistenceEngine;

    Meter putRequests = Metrics.newMeter(AntiEntropyServiceHandler.class,
                                         "put-requests",
                                         "requests",
                                         TimeUnit.SECONDS);

    Meter ackTransactionPending = Metrics.newMeter(AntiEntropyServiceHandler.class,
                                                   "ack-transaction-pending-requests",
                                                   "requests",
                                                   TimeUnit.SECONDS);


    public AntiEntropyServiceHandler(AntiEntropyServiceRouter router,
            DependencyResolver dependencyResolver, IPersistenceEngine persistenceEngine) {
        this.dependencyResolver = dependencyResolver;
        this.router = router;
        this.persistenceEngine = persistenceEngine;
    }

    @Override
    public void put(String key,
                    ThriftDataItem valueThrift) throws TException{
        putRequests.mark();
    	logger.trace("Received anti-entropy put for key " + key);
        DataItem value = new DataItem(valueThrift);
        if (value.getTransactionKeys() == null || value.getTransactionKeys().isEmpty()) {
            persistenceEngine.put_if_newer(key, value);
        } else {
            dependencyResolver.addPendingWrite(key, value);
        }
    }

    @Override
    public void ackTransactionPending(List<ThriftVersion> transactionIds) throws TException {
        ackTransactionPending.mark();
        for (ThriftVersion transactionId : transactionIds) {
            dependencyResolver.ackTransactionPending(Version.fromThrift(transactionId));
        }
    }
}