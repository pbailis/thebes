package edu.berkeley.thebes.common.config;

import java.io.FileNotFoundException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import javax.naming.ConfigurationException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.yammer.metrics.reporting.ConsoleReporter;
import com.yammer.metrics.reporting.GraphiteReporter;

import edu.berkeley.thebes.common.config.ConfigParameterTypes.AtomicityLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.IsolationLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.PersistenceEngine;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.SessionLevel;
import edu.berkeley.thebes.common.config.ConfigParameterTypes.TransactionMode;
import edu.berkeley.thebes.common.thrift.ServerAddress;

public class Config {
    private static TransactionMode txnMode;
    private static List<ServerAddress> clusterServers;
    private static List<ServerAddress> siblingServers = null;
    private static List<ServerAddress> masterServers;
    
    private static String HOST_NAME;

    private static void initialize(List<ConfigParameters> requiredParams) throws FileNotFoundException, ConfigurationException {
        YamlConfig.initialize((String) getOptionNoYaml(ConfigParameters.CONFIG_FILE));

        List<ConfigParameters> missingFields = Lists.newArrayList();
        for(ConfigParameters param : requiredParams) {
            if(getOption(param) == null)
                missingFields.add(param);
        }

        if(missingFields.size() > 0)
            throw new ConfigurationException("missing required configuration options: "+missingFields);
        
    	try {
    		HOST_NAME = InetAddress.getLocalHost().getHostName();
    	} catch (UnknownHostException e) {
    		HOST_NAME = "unknown-host";
    		e.printStackTrace();
    	}

        if (txnMode == null)
            txnMode = getThebesTxnMode();
        clusterServers = getServersInCluster(getClusterID());
        masterServers = getMasterServers();
        
        configureGraphite();
    }
    
    private static void configureGraphite() {
    	String graphiteIP = getOption(ConfigParameters.GRAPHITE_IP);
        if (graphiteIP == null) {
        	return;
        }
    	
        GraphiteReporter.enable(1, TimeUnit.MINUTES, graphiteIP, 2003, HOST_NAME);
    }

    public static void initializeClient() throws FileNotFoundException, ConfigurationException {
        initialize(RequirementLevel.CLIENT_COMMON.getRequiredParameters());
    }

    public static void initializeServer(TransactionMode mode) throws FileNotFoundException, ConfigurationException {
        txnMode = mode;
        initialize(RequirementLevel.SERVER_COMMON.getRequiredParameters());
        siblingServers = getSiblingServers(getClusterID(), getServerID());
    }
    
    public static void initializeTwoPLTransactionManager()
            throws FileNotFoundException, ConfigurationException {
        txnMode = TransactionMode.TWOPL;
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
        if (ret != null) {
            return (T) option.castValue(ret);
        }

        ret = YamlConfig.getOption(option.getTextName());
        if (ret != null)
            return (T) option.castValue(ret);
        
        ret = option.getDefaultValue();
        if (ret != null)
            return (T) ret;
        else
            throw new IllegalStateException("No configuration for " + option);
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

    public static Integer getClusterID() {
        return getOption(ConfigParameters.CLUSTERID);
    }
    
    /** Returns the cluster map (based on the current transaction mode). */
    private static Map<Integer, List<String>> getClusterMap() {
        return getOption(txnMode.getClusterConfigParam());
    }

    private static List<ServerAddress> getServersInCluster(int clusterID) {
        List<String> serverIPs = getClusterMap().get(clusterID);
        List<ServerAddress> servers = Lists.newArrayList();
        
        for (int serverID = 0; serverID < serverIPs.size(); serverID ++) {
            String ip = serverIPs.get(serverID);
            if (ip.endsWith("*")) {
                ip = ip.substring(0, ip.length()-1);
            }
            servers.add(new ServerAddress(clusterID, serverID, ip, getServerPort()));
        }
        return servers;
    }

    private static Integer getServerID() {
        return getOption(ConfigParameters.SERVERID);
    }

    public static Short getClientID() {
        return getOption(ConfigParameters.CLIENTID);
    }

    private static List<ServerAddress> getSiblingServers(int clusterID, int serverID) {
        List<ServerAddress> ret = Lists.newArrayList();
        Map<Integer, List<String>> clusterMap = getClusterMap();
        for (int clusterKey : clusterMap.keySet()) {
            if (clusterKey == clusterID)
                continue;

            String server = clusterMap.get(clusterKey).get(serverID);
            if (txnMode == TransactionMode.TWOPL && server.endsWith("*")) {
                server = server.substring(0, server.length()-1);
            }
            ret.add(new ServerAddress(clusterKey, serverID, server, getServerPort()));
        }
        return ret;
    }
    
    /**
     * Returns the ordered list of Master servers for each serverId.
     * This returns null in HAT mode.
     */
    public static List<ServerAddress> getMasterServers() {
        if (txnMode == TransactionMode.HAT) {
            return null;
        }
        if (masterServers != null) {
            return masterServers;
        }

        Map<Integer, ServerAddress> masterMap = Maps.newHashMap();

        Map<Integer, List<String>> clusterMap = getClusterMap();
        for (int clusterID : clusterMap.keySet()) {
            for (int serverID = 0; serverID < clusterMap.get(clusterID).size(); serverID ++) {
                String server = clusterMap.get(clusterID).get(serverID);
                if (server.endsWith("*")) {
                    assert !masterMap.containsKey(serverID) : "2 masters for serverID " + serverID;
                    masterMap.put(serverID, 
                            new ServerAddress(clusterID, serverID,
                                    server.substring(0, server.length()-1),
                                    getServerPort()));
                }
            }
        }
        
        List<ServerAddress> masters = Lists.newArrayListWithCapacity(clusterServers.size());
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
        return new InetSocketAddress(getServerIP(), getServerPort());
    }

    /** Returns the TM bind ip for the TM in *this* cluster. */
    public static InetSocketAddress getTwoPLTransactionManagerBindIP() {
        Map<Integer, String> tmConfig = getOption(ConfigParameters.TWOPL_TM_CONFIG);
        String myIP = tmConfig.get(getClusterID());
        return new InetSocketAddress(myIP, getTwoPLTransactionManagerPort());
    }
    
    public static ServerAddress getTwoPLTransactionManagerByCluster(int clusterID) {
        Map<Integer, String> tmConfig = getOption(ConfigParameters.TWOPL_TM_CONFIG);
        return new ServerAddress(clusterID, -1,
                tmConfig.get(clusterID), getTwoPLTransactionManagerPort());
    }
    
    public static Boolean shouldReplicateToTwoPLSlaves() {
        return getOption(ConfigParameters.TWOPL_REPLICATE_TO_SLAVES);
    }

    public static List<ServerAddress> getServersInCluster() {
        return clusterServers;
    }

    public static List<ServerAddress> getSiblingServers() {
        return siblingServers;
    }

    public static String getPrettyServerID() {
        return String.format("C%d:S%d", getClusterID(), getServerID());
    }

    public static Boolean isStandaloneServer() {
        return getOption(ConfigParameters.STANDALONE);
    }

    public static TransactionMode getThebesTxnMode() {
        return getOption(ConfigParameters.TXN_MODE);
    }

    public static IsolationLevel getThebesIsolationLevel() {
        return getOption(ConfigParameters.HAT_ISOLATION_LEVEL);
    }

    public static AtomicityLevel getThebesAtomicityLevel() {
        return getOption(ConfigParameters.ATOMICITY_LEVEL);
    }

    public static SessionLevel getThebesSessionLevel() {
        return getOption(ConfigParameters.SESSION_LEVEL);
    }

    /** Returns true if this server is the Master of a 2PL replica set. */
    public static Boolean isMaster() {
        return txnMode == TransactionMode.TWOPL &&
                masterServers.get(getServerID()).getIP().equals(getServerIP());
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