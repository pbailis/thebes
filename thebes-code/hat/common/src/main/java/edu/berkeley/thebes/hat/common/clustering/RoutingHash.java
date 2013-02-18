package edu.berkeley.thebes.hat.common.clustering;

public class RoutingHash {
    public static int hashKey(String key, int numServers) {
        return key.hashCode() % numServers;
    }
}