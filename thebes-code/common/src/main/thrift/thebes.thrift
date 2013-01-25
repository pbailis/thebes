#!/usr/local/bin/thrift --gen cpp

namespace java edu.berkeley.thebes.common.thrift

service ReplicaService {
  binary get(1: string key);
  bool put(1: string key, 2: binary value);
}