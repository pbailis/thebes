#!/usr/local/bin/thrift --gen java

include "dataitem.thrift"

namespace java edu.berkeley.thebes.hat.common.thrift

struct DataDependency {
  1: string key,
  2: i64 timestamp
}

service ReplicaService {
  dataitem.DataItem get(1: string key);
  bool put(1: string key, 2: dataitem.DataItem value, 3: list<DataDependency> happensAfter);
}

service AntiEntropyService {
  oneway void put(1: string key, 2: dataitem.DataItem value, 3: list<DataDependency> happensAfter);
  void waitForDependency(1: DataDependency dependency);
}