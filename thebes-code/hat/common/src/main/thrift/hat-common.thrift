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
           3: list<string> transactionKeys);
}

service AntiEntropyService {
   oneway void put(1: string key,
                  2: dataitem.ThriftDataItem value,
                  3: list<string> transactionKeys);

  void waitForTransactionalDependency(1: ThriftDataDependency dependency);
}