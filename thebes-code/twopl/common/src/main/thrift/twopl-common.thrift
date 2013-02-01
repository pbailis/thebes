#!/usr/local/bin/thrift --gen java

include "dataitem.thrift"

namespace java edu.berkeley.thebes.twopl.common.thrift

service TwoPLMasterReplicaService {
  bool lock(1: i64 sessionId, 2: string key);
  bool unlock(1: i64 sessionId, 2: string key);
  dataitem.DataItem get(1: i64 sessionId, 2: string key);
  bool put(1: i64 sessionId, 2: string key, 3: dataitem.DataItem value);
}
