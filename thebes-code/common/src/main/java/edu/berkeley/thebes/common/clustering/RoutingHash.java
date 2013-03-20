package edu.berkeley.thebes.common.clustering;

public class RoutingHash {
    public static int hashKey(String key, int numServers) {
        try {
            return Integer.parseInt(""+key.charAt(key.length()-1));
        } catch (Exception e) {
            return 0;
        }
        // TODODODO
//        return Math.abs(key.hashCode()) % numServers;
    }
}
