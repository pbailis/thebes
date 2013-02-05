package edu.berkeley.thebes.common.thrift;

public class ServerAddress {
    private final int clusterID;
    private final int serverID;
    private final String ip;
    private final int port;
    
    public ServerAddress(int clusterID, int serverID, String ip, int port) {
        super();
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
    
}
