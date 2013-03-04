package edu.berkeley.thebes.hat.server.antientropy;

import java.util.List;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.berkeley.thebes.common.data.DataItem;
import edu.berkeley.thebes.common.thrift.ThriftDataItem;
import edu.berkeley.thebes.hat.common.data.DataDependency;
import edu.berkeley.thebes.hat.common.thrift.AntiEntropyService;
import edu.berkeley.thebes.hat.common.thrift.ThriftDataDependency;
import edu.berkeley.thebes.hat.server.dependencies.DependencyResolver;

public class AntiEntropyServiceHandler implements AntiEntropyService.Iface {
    private static Logger logger = LoggerFactory.getLogger(AntiEntropyServiceHandler.class);
    
    DependencyResolver dependencyResolver;

    public AntiEntropyServiceHandler(DependencyResolver dependencyResolver) {
        this.dependencyResolver = dependencyResolver;
    }

    @Override
    public void put(String key,
                    ThriftDataItem value,
                    List<String> transactionKeys) throws TException{
    	logger.debug("Received anti-entropy put for key!");
        dependencyResolver.asyncApplyNewWrite(key,
                                              DataItem.fromThrift(value),
                                              transactionKeys);
    }

    @Override
    public void waitForTransactionalDependency(ThriftDataDependency dependency) {
        dependencyResolver.blockForAtomicDependency(DataDependency.fromThrift(dependency));
    }
}