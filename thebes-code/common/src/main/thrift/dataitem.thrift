#!/usr/local/bin/thrift --gen java

namespace java edu.berkeley.thebes.common.thrift

struct DataItem {
  1: binary data,
  2: i64 timestamp
}
