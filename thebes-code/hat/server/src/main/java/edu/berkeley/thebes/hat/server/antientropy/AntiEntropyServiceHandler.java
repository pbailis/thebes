package edu.berkeley.thebes.hat.server.antientropy;

import edu.berkeley.thebes.common.thrift.DataItem;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.DataDependency;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;
import org.apache.thrift.TException;

import java.util.List;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    DependencyResolver dependencyResolver;

    public AntiEntropyServiceHandler(DependencyResolver dependencyResolver) {
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public void put(String key,
                    DataItem value,
                    List<DataDependency> happensAfter,
                    List<String> transactionKeys) throws TException{

        dependencyResolver.asyncApplyNewRemoteWrite(key,
                                                    value,
                                                    happensAfter,
                                                    transactionKeys);
    }

    @Override
    public void waitForCausalDependency(DataDependency dependency) {
        dependencyResolver.blockForCausalDependency(dependency);
    }

    @Override
    public void waitForTransactionalDependency(DataDependency dependency) {
        dependencyResolver.blockForAtomicDependency(dependency);
    }
}