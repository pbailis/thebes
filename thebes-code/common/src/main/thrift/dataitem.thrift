#!/usr/local/bin/thrift --gen java

namespace java edu.berkeley.thebes.common.thrift

include "version.thrift"

struct DataItem {
  1: binary data,
  2: version.Version version,
  4: optional list<string> transactionKeys
}
