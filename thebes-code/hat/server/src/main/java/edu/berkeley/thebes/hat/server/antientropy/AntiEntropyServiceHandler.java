package edu.berkeley.thebes.hat.server.antientropy;

import edu.berkeley.thebes.common.persistence.IPersistenceEngine;
import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import edu.berkeley.thebes.hat.server.dependencies.GenericDependencyChecker;
import edu.berkeley.thebes.hat.server.dependencies.GenericDependencyResolver;
import org.apache.thrift.TException;

import java.util.List;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private IPersistenceEngine persistenceEngine;
    GenericDependencyChecker causalDependencyChecker;
    GenericDependencyResolver causalDependencyResolver;

    public AntiEntropyServiceHandler(IPersistenceEngine persistenceEngine,
                                     GenericDependencyChecker causalDependencyChecker,
                                     GenericDependencyResolver causalDependencyResolver) {
        this.persistenceEngine = persistenceEngine;
        this.causalDependencyChecker = causalDependencyChecker;
        this.causalDependencyResolver = causalDependencyResolver;
    }

    @Override
    public void put(String key, DataItem value, List<DataDependency> happensAfter) throws TException{
        if(happensAfter.isEmpty())
            persistenceEngine.put(key, value);
        else
            causalDependencyChecker.applyWriteAfterDependencies(key, value, happensAfter);
    }

    @Override
    public void waitForDependency(DataDependency dependency) {
        causalDependencyResolver.blockForDependency(dependency);
    }
}