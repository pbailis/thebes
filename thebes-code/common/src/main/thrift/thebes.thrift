#!/usr/local/bin/thrift --gen cpp

namespace java edu.berkeley.thebes.common.thrift

service ThebesReplicaService {
  binary get(1: string key);
  bool put(1: string key, 2: binary value);
}