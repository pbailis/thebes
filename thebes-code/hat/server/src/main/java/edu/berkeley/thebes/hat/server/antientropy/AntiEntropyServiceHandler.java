package edu.berkeley.thebes.hat.server.antientropy;

import java.util.List;

import org.apache.thrift.TException;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftDataDependency;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    DependencyResolver dependencyResolver;

    public AntiEntropyServiceHandler(DependencyResolver dependencyResolver) {
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public void put(String key,
                    ThriftDataItem value,
                    List<String> transactionKeys) throws TException{

        dependencyResolver.asyncApplyNewWrite(key,
                                              DataItem.fromThrift(value),
                                              transactionKeys);
    }

    @Override
    public void waitForTransactionalDependency(ThriftDataDependency dependency) {
        dependencyResolver.blockForAtomicDependency(DataDependency.fromThrift(dependency));
    }
}