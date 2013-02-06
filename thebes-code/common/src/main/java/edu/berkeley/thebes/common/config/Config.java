package edu.berkeley.thebes.common.config;

import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.naming.ConfigurationException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import edu.berkeley.thebes.common.config.ConfigParameterTypes.IsolationLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.TransactionMode;

public class Config {
    private static TransactionMode txnMode;
    private static List<String> clusterServers;
    private static List<String> neighborServers = null;
    private static List<String> masterServers;

    private static void initialize(List<ConfigParameters> requiredParams) throws FileNotFoundException, ConfigurationException {
        YamlConfig.initialize((String) getOptionNoYaml(ConfigParameters.CONFIG_FILE));

        List<ConfigParameters> missingFields = Lists.newArrayList();
        for(ConfigParameters param : requiredParams) {
            if(getOption(param) == null)
                missingFields.add(param);
        }

        if(missingFields.size() > 0)
            throw new ConfigurationException("missing required configuration options: "+missingFields);

        if (txnMode == null)
            txnMode = getThebesTxnMode();
        clusterServers = getServersInCluster(getClusterID());
        masterServers = getMasterServers();
    }

    public static void initializeClient() throws FileNotFoundException, ConfigurationException {
        initialize(RequirementLevel.CLIENT_COMMON.getRequiredParameters());
    }

    public static void initializeServer(TransactionMode mode) throws FileNotFoundException, ConfigurationException {
        txnMode = mode;
        initialize(RequirementLevel.SERVER_COMMON.getRequiredParameters());
        neighborServers = getSiblingServers(getClusterID(), getServerID());
    }
    
    public static void initializeTwoPLTransactionManager()
            throws FileNotFoundException, ConfigurationException {
        txnMode = TransactionMode.TWOPL;
        // TODO: Be aware that the TM depends on common... should probably restructure some time
        initialize(RequirementLevel.TWOPL_TM.getRequiredParameters());
    }

    public Config() throws FileNotFoundException, ConfigurationException {
        clusterServers = getServersInCluster(getClusterID());
    }
    
    private static Object getOptionNoYaml(ConfigParameters option) {
        Object ret = System.getProperty(option.getTextName());
        if (ret != null)
            return option.castValue(ret);

        return option.getDefaultValue();
    }

    public static <T> T getOption(ConfigParameters option) {
        Object ret = System.getProperty(option.getTextName());
        if (ret != null)
            return (T) option.castValue(ret);

        ret = YamlConfig.getOption(option.getTextName());
        
        if (ret != null)
            return (T) option.castValue(ret);
        
        return (T) option.getDefaultValue();
    }

    public static PersistenceEngine getPersistenceType() {
        return getOption(ConfigParameters.PERSISTENCE_ENGINE);
    }

    public static Integer getServerPort() {
        return getOption(ConfigParameters.SERVER_PORT);
    }
    
    public static Integer getAntiEntropyServerPort() {
        return getOption(ConfigParameters.ANTI_ENTROPY_PORT);
    }
    
    public static Integer getTwoPLServerPort() {
        return getOption(ConfigParameters.TWOPL_PORT);
    }

    private static Integer getTwoPLTransactionManagerPort() {
        return getOption(ConfigParameters.TWOPL_TM_PORT);
    }

    private static Integer getClusterID() {
        return getOption(ConfigParameters.CLUSTERID);
    }
    
    /** Returns the cluster map (based on the current transaction mode). */
    @SuppressWarnings("unchecked")
    private static Map<Integer, List<String>> getClusterMap() {
        return getOption(txnMode.getClusterConfigParam());
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

    private static Integer getServerID() {
        return getOption(ConfigParameters.SERVERID);
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

    public static Integer getSocketTimeout() {
        return getOption(ConfigParameters.SOCKET_TIMEOUT);
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

    public static InetSocketAddress getTwoPLTransactionManagerBindIP() {
        return new InetSocketAddress(
                (String) getOption(ConfigParameters.TWOPL_TM_IP),
                getTwoPLTransactionManagerPort());
    }
    
    public static Boolean shouldReplicateToTwoPLSlaves() {
        return getOption(ConfigParameters.TWOPL_REPLICATE_TO_SLAVES);
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
        return getOption(ConfigParameters.STANDALONE);
    }

    public static TransactionMode getThebesTxnMode() {
        return getOption(ConfigParameters.TXN_MODE);
    }

    public static IsolationLevel getThebesIsolationLevel() {
        return getOption(ConfigParameters.HAT_ISOLATION_LEVEL);
    }
    
    /** Returns true if this server is the Master of a 2PL replica set. */
    public static Boolean isMaster() {
        return txnMode == TransactionMode.TWOPL &&
                masterServers.get(getServerID()).equals(getServerIP());
    }
    
    /** Returns the IP for this server, based on our clusterid and serverid. */
    private static String getServerIP() {
        String ip = getClusterMap().get(getClusterID()).get(getServerID());
        if (ip.endsWith("*")) {
            return ip.substring(0, ip.length()-1);
        } else {
            return ip;
        }
    }
}