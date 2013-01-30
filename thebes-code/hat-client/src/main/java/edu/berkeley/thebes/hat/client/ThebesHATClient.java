package edu.berkeley.thebes.hat.client;

import edu.berkeley.thebes.common.clustering.ReplicaRouter;
import edu.berkeley.thebes.common.config.Config;
import edu.berkeley.thebes.common.thrift.ReplicaService;
import edu.berkeley.thebes.common.thrift.ThriftUtil;

import java.nio.ByteBuffer;

public class ThebesHATClient {
    public static void main(String[] args) {
        try {
            Config.initializeClientConfig(args);

            ReplicaRouter router = new ReplicaRouter();

            System.out.println(router.getReplicaByKey("foo").put("foo", ByteBuffer.wrap("foobar".getBytes())));
            ByteBuffer ret = router.getReplicaByKey("asdfasdf").get("asdfasdf");
            System.out.println(new String(ret.array()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}