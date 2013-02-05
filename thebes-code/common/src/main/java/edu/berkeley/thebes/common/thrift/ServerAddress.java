package edu.berkeley.thebes.common.thrift;

/** Simply stores all the attributes of some external server. */
public class ServerAddress {
    private final int clusterID;
    private final int serverID;
    private final String ip;
    private int port;
    
    public ServerAddress(int clusterID, int serverID, String ip, int port) {
        this.clusterID = clusterID;
        this.serverID = serverID;
        this.ip = ip;
        this.port = port;
    }

    public int getClusterID() {
        return clusterID;
    }

    public int getServerID() {
        return serverID;
    }

    public String getIP() {
        return ip;
    }

    public int getPort() {
        return port;
    }
    
    @Override
    public String toString() {
        return ip + ":" + port + " [" + clusterID + ", " + serverID + "]";
    }
}
