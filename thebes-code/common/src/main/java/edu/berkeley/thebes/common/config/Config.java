package edu.berkeley.thebes.common.config;

import edu.berkeley.thebes.common.config.commandline.ClientCommandLineConfigOptions;
import edu.berkeley.thebes.common.config.commandline.CommonCommandLineConfigOptions;
import edu.berkeley.thebes.common.config.commandline.ServerCommandLineConfigOptions;

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


    public static void initializeClientConfig(String[] commandLine) throws FileNotFoundException,
                                                                           ConfigurationException {
        CommandLineConfig.initialize(CommandLineConfig.combineOptions(
                ClientCommandLineConfigOptions.constructClientOptions(),
                CommonCommandLineConfigOptions.constructCommonOptions()),
                                     commandLine);
        YamlConfig.initialize(CommandLineConfig.getOption("config"));

        clusterServers = getServersInCluster(getClusterID());
    }

    public static void initializeServerConfig(String[] commandLine)
            throws FileNotFoundException, ConfigurationException {
        CommandLineConfig.initialize(CommandLineConfig.combineOptions(
                ServerCommandLineConfigOptions.constructServerOptions(),
                CommonCommandLineConfigOptions.constructCommonOptions()),
                                     commandLine);
        YamlConfig.initialize(CommandLineConfig.getOption("config"));

        clusterServers = getServersInCluster(getClusterID());
        neighborServers = getSiblingServers(getClusterID(), getServerID());
    }

    private static Object getOption(String optionName) {
        Object ret = CommandLineConfig.getOption(optionName);
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

    public static String getPersistenceType() {
        return (String) getOption(ConfigStrings.PERSISTENCE_ENGINE, ConfigDefaults.PERSISTENCE_ENGINE);
    }

    public static int getServerPort() {
        return (Integer) getOption("port", ConfigDefaults.SERVER_PORT);
    }

    private static int getClusterID() {
        return Integer.parseInt(CommandLineConfig.getOption(ConfigStrings.CLUSTER_ID));
    }

    private static List<String> getServersInCluster(int clusterID) {
        return (List) ((Map) YamlConfig.getOption(ConfigStrings.CLUSTER_CONFIG)).get(clusterID);
    }

    private static int getServerID() {
        return Integer.parseInt(CommandLineConfig.getOption(ConfigStrings.SERVER_ID));
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
        return CommandLineConfig.hasOption(ConfigStrings.STANDALONE_MODE);
    }
}