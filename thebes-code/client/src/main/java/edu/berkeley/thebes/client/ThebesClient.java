package edu.berkeley.thebes.client;

import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.common.thrift.ThriftUtil;

import java.nio.ByteBuffer;

public class ThebesClient {
    public static void main(String[] args) {
        try {
            Config.initializeClientConfig(args);

            ReplicaService.Client client = ThriftUtil.getReplicaServiceClient("127.0.0.1", 8080);

            System.out.println(client.put("foo", ByteBuffer.wrap("foobar".getBytes())));
            ByteBuffer ret = client.get("asdfasdf");
            System.out.println(new String(ret.array()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}