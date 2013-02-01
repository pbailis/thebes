package edu.berkeley.thebes.common.config;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Config {
    private static List<String> clusterServers;
    private static List<String> neighborServers = null;

    private static void initialize(List<String> requiredFields) throws FileNotFoundException, ConfigurationException {
        YamlConfig.initialize(System.getProperty(ConfigStrings.CONFIG_FILE, ConfigDefaults.CONFIG_LOCATION));

        List<String> missingFields = new ArrayList<String>();
        for(String option : requiredFields) {
            if(getOption(option) == null)
                missingFields.add(option);
        }

        if(missingFields.size() > 0)
            throw new ConfigurationException("missing required configuration options: "+missingFields);

        clusterServers = getServersInCluster(getClusterID());
    }

    public static void initializeClient() throws FileNotFoundException, ConfigurationException {
        initialize(ConfigStrings.requiredClientConfigOptions);
    }

    public static void initializeServer() throws FileNotFoundException, ConfigurationException {
        initialize(ConfigStrings.requiredServerConfigOptions);

        neighborServers = getSiblingServers(getClusterID(), getServerID());
    }

    public Config() throws FileNotFoundException,
                                                                           ConfigurationException {

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

    private static int getClusterID() {
        return getIntegerOption(ConfigStrings.CLUSTER_ID);
    }

    private static List<String> getServersInCluster(int clusterID) {
        return (List) ((Map) YamlConfig.getOption(ConfigStrings.CLUSTER_CONFIG)).get(clusterID);
    }

    private static int getServerID() {
        return getIntegerOption(ConfigStrings.SERVER_ID);
    }

    private static List<String> getSiblingServers(int clusterID, int serverID) {
        List<String> ret = new ArrayList<String>();
        Map clusterMap = ((Map) YamlConfig.getOption(ConfigStrings.CLUSTER_CONFIG));
        for (int clusterKey : (Set<Integer>) clusterMap.keySet()) {
            if (clusterKey == clusterID)
                continue;

            ret.add(((List<String>) clusterMap.get(clusterKey)).get(serverID));
        }
        return ret;
    }

    public static int getSocketTimeout() {
        return (Integer) getOption(ConfigStrings.SOCKET_TIMEOUT, ConfigDefaults.SOCKET_TIMEOUT);
    }

    public static InetSocketAddress getServerBindIP() {
        return new InetSocketAddress((String) getOption(ConfigStrings.SERVER_BIND_IP,
                                                        ConfigDefaults.SERVER_BIND_IP),
                                     getServerPort());
    }

    public static InetSocketAddress getAntiEntropyServerBindIP() {
        return new InetSocketAddress((String) getOption(ConfigStrings.SERVER_BIND_IP,
                                                        ConfigDefaults.SERVER_BIND_IP),
                                     getAntiEntropyServerPort());
    }

    //todo: should change this to include port numbers as well
    public static List<String> getServersInCluster() {
        return clusterServers;
    }

    //todo: should change this to include port numbers as well
    public static List<String> getNeighborServers() {
        return neighborServers;
    }

    public static String getPrettyServerID() {
        return String.format("C%d:S%d", getClusterID(), getServerID());
    }

    public static boolean isStandaloneServer() {
        return getOption(ConfigStrings.STANDALONE_MODE) != null;
    }

    public static String getThebesTxnMode() {
        return (String) getOption(ConfigStrings.TXN_MODE, ConfigDefaults.THEBES_TXN_MODE);
    }
}