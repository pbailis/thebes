package edu.berkeley.thebes.server;

import edu.berkeley.thebes.common.thrift.ThebesReplicaService;

import java.nio.ByteBuffer;

public class ThebesReplicaServiceHandler implements ThebesReplicaService.Iface {

    public boolean put(String key, ByteBuffer value) {
        return true;
    }

    @Override
    public ByteBuffer get(String key) {
        return ByteBuffer.wrap("foo".getBytes());
    }
}