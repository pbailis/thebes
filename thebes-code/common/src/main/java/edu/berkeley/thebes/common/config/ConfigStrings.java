package edu.berkeley.thebes.common.config;

import java.util.ArrayList;
import java.util.List;

public class ConfigStrings {
    public static final String CONFIG_FILE = "config";
    public static final String CLUSTER_ID = "clusterid";
    //0-N-1, with exactly N servers per cluster
    public static final String SERVER_ID = "serverid";
    public static final String CLUSTER_CONFIG = "cluster_configuration";
    public static final String PERSISTENCE_ENGINE = "persistence_engine";
    public static final String PERSISTENCE_MEMORY = "memory";
    public static final String SOCKET_TIMEOUT = "socket_timeout_ms";
    public static final String SERVER_BIND_IP = "ip";
    public static final String SERVER_PORT = "port";
    public static final String ANTI_ENTROPY_PORT = "anti_entropy_port";
    public static final String STANDALONE_MODE = "standalone";
    public static final String TXN_MODE = "txn_mode";
    public static final String HAT_MODE = "hat";
    public static final String TWOPL_MODE = "twopl";

    public static final List<String> requiredCommonConfigOptions = new ArrayList<String>() {{
        add(CLUSTER_ID);
    }};

    public static final List<String> requiredClientConfigOptions = new ArrayList<String>() {{
        addAll(requiredCommonConfigOptions);
    }};

    public static final List<String> requiredServerConfigOptions = new ArrayList<String>() {{
        addAll(requiredCommonConfigOptions);
        add(SERVER_ID);
    }};
}