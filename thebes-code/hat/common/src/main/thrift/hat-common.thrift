#!/usr/local/bin/thrift --gen java

include "dataitem.thrift"
include "version.thrift"

namespace java edu.berkeley.thebes.hat.common.thrift

struct DataDependency {
  1: string key,
  2: version.Version version
}

service ReplicaService {
  dataitem.DataItem get(1: string key
                        2: version.Version requiredVersion);

  bool put(1: string key,
           2: dataitem.DataItem value,
           3: list<DataDependency> happensAfter,
           4: list<string> transactionKeys);
}

service AntiEntropyService {
  oneway void put(1: string key,
                  2: dataitem.DataItem value,
                  3: list<DataDependency> happensAfter,
                  4: list<string> transactionKeys);

  void waitForCausalDependency(1: DataDependency dependency);
  void waitForTransactionalDependency(1: DataDependency dependency);
}