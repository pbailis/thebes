#!/usr/local/bin/thrift --gen java

namespace java edu.berkeley.thebes.common.thrift

struct DataItem {
  1: binary data,
  2: i64 timestamp
}

service ReplicaService {
  DataItem get(1: string key);
  bool put(1: string key, 2: DataItem value);
}

service AntiEntropyService {
  bool put(1: string key, 2: DataItem value);
}