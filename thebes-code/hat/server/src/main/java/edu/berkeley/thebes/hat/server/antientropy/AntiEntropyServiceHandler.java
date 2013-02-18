package edu.berkeley.thebes.hat.server.antientropy;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import edu.berkeley.thebes.hat.server.causal.CausalDependencyChecker;
import edu.berkeley.thebes.hat.server.causal.CausalDependencyResolver;
import org.apache.thrift.TException;

import java.util.List;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private IPersistenceEngine persistenceEngine;
    CausalDependencyResolver resolver;
    CausalDependencyChecker checker;

    public AntiEntropyServiceHandler(IPersistenceEngine persistenceEngine,
                                     CausalDependencyChecker checker,
                                     CausalDependencyResolver resolver) {
        this.persistenceEngine = persistenceEngine;
        this.checker = checker;
        this.resolver = resolver;
    }

    @Override
    public void put(String key, DataItem value, List<DataDependency> happensAfter) throws TException{
        if(happensAfter.isEmpty())
            persistenceEngine.put(key, value);
        else
            checker.applyWriteAfterDependencies(key, value, happensAfter);
    }

    @Override
    public void waitForDependency(DataDependency dependency) {
        resolver.blockForDependency(dependency);
    }
}