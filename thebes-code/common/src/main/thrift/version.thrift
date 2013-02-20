#!/usr/local/bin/thrift --gen java

namespace java edu.berkeley.thebes.common.thrift

struct ThriftVersion {
  1: i16 clientID,
  3: i64 timestamp,
}
