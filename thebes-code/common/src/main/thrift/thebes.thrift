#!/usr/local/bin/thrift --gen cpp

namespace cpp thebes

service ThebesServer {
  binary get(1: string key);
  bool put(1: string key, 2: binary value);
}