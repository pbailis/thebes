package edu.berkeley.thebes.common.config;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.naming.ConfigurationException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class Config {
    public enum TransactionMode {
        HAT (ConfigStrings.CLUSTER_CONFIG),
        TWOPL (ConfigStrings.TWOPL_CLUSTER_CONFIG);
        
        private final String clusterConfigString;

        private TransactionMode(String clusterConfigString) {
            this.clusterConfigString = clusterConfigString;
        }

        public String getClusterConfigString() {
            return clusterConfigString;
        }
    }
    
    private static TransactionMode txnMode;
    private static List<String> clusterServers;
    private static List<String> neighborServers = null;
    private static List<String> masterServers;

    private static void initialize(List<String> requiredFields) throws FileNotFoundException, ConfigurationException {
        YamlConfig.initialize(System.getProperty(ConfigStrings.CONFIG_FILE, ConfigDefaults.CONFIG_LOCATION));

        List<String> missingFields = new ArrayList<String>();
        for(String option : requiredFields) {
            if(getOption(option) == null)
                missingFields.add(option);
        }

        if(missingFields.size() > 0)
            throw new ConfigurationException("missing required configuration options: "+missingFields);

        if (txnMode == null)
            txnMode = getThebesTxnMode();
        clusterServers = getServersInCluster(getClusterID());
        masterServers = getMasterServers();
    }

    public static void initializeClient() throws FileNotFoundException, ConfigurationException {
        initialize(ConfigStrings.requiredClientConfigOptions);
    }

    public static void initializeServer(TransactionMode mode) throws FileNotFoundException, ConfigurationException {
        txnMode = mode;
        initialize(ConfigStrings.requiredServerConfigOptions);
        neighborServers = getSiblingServers(getClusterID(), getServerID());
    }

    public Config() throws FileNotFoundException, ConfigurationException {
        clusterServers = getServersInCluster(getClusterID());
    }

    private static Object getOption(String optionName) {
        Object ret = System.getProperty(optionName);
        if (ret != null)
            return ret;

        return YamlConfig.getOption(optionName);
    }

    private static Object getOption(String optionName, Object defaultValue) {
        Object ret = getOption(optionName);

        if (ret == null) {
            return defaultValue;
        }

        return ret;
    }

    private static int getIntegerOption(String optionName) {
        Object returnOption = getOption(optionName);

        if(String.class.isInstance(returnOption))
            return Integer.parseInt((String) returnOption);
        else
            return (Integer) returnOption;
    }

    public static String getPersistenceType() {
        return (String) getOption(ConfigStrings.PERSISTENCE_ENGINE, ConfigDefaults.PERSISTENCE_ENGINE);
    }

    public static int getServerPort() {
        return (Integer) getOption(ConfigStrings.SERVER_PORT, ConfigDefaults.SERVER_PORT);
    }
    
    public static int getAntiEntropyServerPort() {
        return (Integer) getOption(ConfigStrings.ANTI_ENTROPY_PORT, ConfigDefaults.ANTI_ENTROPY_PORT);
    }
    
    public static int getTwoPLServerPort() {
        return (Integer) getOption(ConfigStrings.TWOPL_PORT, ConfigDefaults.TWO_PL_PORT);
    }

    private static int getClusterID() {
        return getIntegerOption(ConfigStrings.CLUSTER_ID);
    }
    
    /** Returns the cluster map (based on the current transaction mode). */
    @SuppressWarnings("unchecked")
    private static Map<Integer, List<String>> getClusterMap() {
        return (Map<Integer, List<String>>) YamlConfig.getOption(txnMode.getClusterConfigString());
    }

    private static List<String> getServersInCluster(int clusterID) {
        List<String> servers = getClusterMap().get(clusterID);
        
        // The Master of a 2PL set is signified by an * at the end. We need to remove this.
        if (txnMode == TransactionMode.TWOPL) {
            List<String> serverNames = Lists.newArrayListWithCapacity(servers.size());
            for (String s : servers) {
                if (s.endsWith("*")) {
                    serverNames.add(s.substring(0, s.length()-1));
                } else {
                    serverNames.add(s);
                }
            }
            return serverNames;
        } else {
            return servers;
        }
    }

    private static int getServerID() {
        return getIntegerOption(ConfigStrings.SERVER_ID);
    }

    private static List<String> getSiblingServers(int clusterID, int serverID) {
        List<String> ret = new ArrayList<String>();
        Map<Integer, List<String>> clusterMap = getClusterMap();
        for (int clusterKey : clusterMap.keySet()) {
            if (clusterKey == clusterID)
                continue;

            String server = clusterMap.get(clusterKey).get(serverID);
            if (txnMode == TransactionMode.TWOPL && server.endsWith("*")) {
                server = server.substring(0, server.length()-1);
            }
            ret.add(server);
        }
        return ret;
    }
    
    /**
     * Returns the ordered list of Master servers for each serverId.
     * This returns null in HAT mode.
     */
    public static List<String> getMasterServers() {
        if (txnMode == TransactionMode.HAT) {
            return null;
        }
        if (masterServers != null) {
            return masterServers;
        }

        Map<Integer, String> masterMap = Maps.newHashMap();

        Map<Integer, List<String>> clusterMap = getClusterMap();
        for (int clusterKey : clusterMap.keySet()) {
            for (int serverID = 0; serverID < clusterMap.get(clusterKey).size(); serverID ++) {
                String server = clusterMap.get(clusterKey).get(serverID);
                if (server.endsWith("*")) {
                    assert !masterMap.containsKey(serverID) : "2 masters for serverID " + serverID;
                    masterMap.put(serverID, server.substring(0, server.length()-1));
                }
            }
        }
        
        // Add a little post condition checking, since this could be misconfigured.
        List<String> masters = Lists.newArrayListWithCapacity(clusterServers.size());
        for (int i = 0; i < clusterServers.size(); i ++) {
            assert masterMap.containsKey(i) : "Missing master for replica set " + i;
            masters.add(masterMap.get(i));
        }
        return masters;
    }

    public static int getSocketTimeout() {
        return (Integer) getOption(ConfigStrings.SOCKET_TIMEOUT, ConfigDefaults.SOCKET_TIMEOUT);
    }

    public static InetSocketAddress getServerBindIP() {
        return new InetSocketAddress(getServerIP(), getServerPort());
    }

    public static InetSocketAddress getAntiEntropyServerBindIP() {
        return new InetSocketAddress(getServerIP(), getAntiEntropyServerPort());
    }

    public static InetSocketAddress getTwoPLServerBindIP() {
        return new InetSocketAddress(getServerIP(), getTwoPLServerPort());
    }

    //todo: should change this to include port numbers as well
    public static List<String> getServersInCluster() {
        return clusterServers;
    }

    //todo: should change this to include port numbers as well
    public static List<String> getSiblingServers() {
        return neighborServers;
    }

    public static String getPrettyServerID() {
        return String.format("C%d:S%d", getClusterID(), getServerID());
    }

    public static boolean isStandaloneServer() {
        return getOption(ConfigStrings.STANDALONE_MODE) != null;
    }

    public static TransactionMode getThebesTxnMode() {
        String opt = (String) getOption(ConfigStrings.TXN_MODE, ConfigDefaults.THEBES_TXN_MODE);
        if (ConfigStrings.HAT_MODE.equals(opt)) {
            return TransactionMode.HAT;
        } else if (ConfigStrings.TWOPL_MODE.equals(opt)) {
            return TransactionMode.TWOPL;
        } else {
            throw new IllegalStateException("Incorrect configuration for txn_mode: " + opt);
        }
    }
    
    /** Returns true if this server is the Master of a 2PL replica set. */
    public static boolean isMaster() {
        return txnMode == TransactionMode.TWOPL &&
                masterServers.get(getServerID()).equals(getServerIP());
    }
    
    /** Returns the IP for this server, based on our clusterid and serverid. */
    private static String getServerIP() {
        return getClusterMap().get(getClusterID()).get(getServerID());
    }
}