#!/usr/local/bin/thrift --gen java

include "dataitem.thrift"
include "version.thrift"

namespace java edu.berkeley.thebes.hat.common.thrift

struct ThriftDataDependency {
  1: string key,
  2: version.ThriftVersion version
}

service ReplicaService {
  dataitem.ThriftDataItem get(1: string key
                        2: version.ThriftVersion requiredVersion);

  bool put(1: string key,
           2: dataitem.ThriftDataItem value,
           3: list<ThriftDataDependency> happensAfter,
           4: list<string> transactionKeys);
}

service AntiEntropyService {
   # should be oneway, but oneway is broken in 0.8 (THRIFT-1447)
   # https://issues.apache.org/jira/browse/THRIFT-1447
   void put(1: string key,
                  2: dataitem.ThriftDataItem value,
                  3: list<ThriftDataDependency> happensAfter,
                  4: list<string> transactionKeys);

  void waitForCausalDependency(1: ThriftDataDependency dependency);
  void waitForTransactionalDependency(1: ThriftDataDependency dependency);
}