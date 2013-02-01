#!/usr/local/bin/thrift --gen java

include "dataitem.thrift"

namespace java edu.berkeley.thebes.twopl.common.thrift

service ReplicaService {
  dataitem.DataItem get(1: string key);
  bool put(1: string key, 2: dataitem.DataItem value);
}
