package edu.berkeley.thebes.hat.server.dependencies;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import edu.berkeley.thebes.hat.server.antientropy.clustering.AntiEntropyServiceRouter;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/* Handles all logic for checking causal dependencies of remote writes */
public class GenericDependencyChecker {
    private IPersistenceEngine localStore;
    private AntiEntropyServiceRouter router;
    private GenericDependencyResolver resolver;
    private static Logger logger = LoggerFactory.getLogger(
            GenericDependencyChecker.class);

    private class DependencyCallback implements
            AsyncMethodCallback<AntiEntropyService.AsyncClient.waitForDependency_call> {
        String key;
        DataItem toApply;
        AtomicInteger blockFor;

        public DependencyCallback(String key, DataItem toApply, int blockFor) {
            this.key = key;
            this.toApply = toApply;
            this.blockFor = new AtomicInteger(blockFor);
        }

        public void onComplete(AntiEntropyService.AsyncClient.waitForDependency_call waitForDependency_call) {
            if(blockFor.decrementAndGet() == 0) {
                localStore.put(key, toApply);
                resolver.notifyNewLocalWrite(key, toApply);
            }
        }

        public void onError(Exception e) {
            logger.warn(e.getMessage());
            //TODO: rethink this behavior
            blockFor.decrementAndGet();
        }
    }

    public GenericDependencyChecker(IPersistenceEngine localStore,
                                    AntiEntropyServiceRouter router,
                                    GenericDependencyResolver resolver) {
        this.localStore = localStore;
        this.router = router;
        this.resolver = resolver;
    }

    public void applyWriteAfterDependencies(String key,
                                            DataItem newItem,
                                            List<DataDependency> happensAfter) throws TException {
        assert(!happensAfter.isEmpty());

        if(localStore.get(key).getTimestamp() > newItem.getTimestamp())
            return;

        DependencyCallback callback = new DependencyCallback(key, newItem, happensAfter.size());

        for(DataDependency dependency : happensAfter) {
            router.getReplicaByKey(dependency.getKey()).waitForDependency(dependency, callback);
        }
    }
}
