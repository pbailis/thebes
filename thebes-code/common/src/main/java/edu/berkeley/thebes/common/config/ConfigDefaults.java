package edu.berkeley.thebes.common.config;

public class ConfigDefaults {
    protected final static String CONFIG_LOCATION = "conf/thebes.yaml";
    protected final static int SERVER_PORT = 8080;
    protected final static int ANTI_ENTROPY_PORT = 8081;
    protected final static int TWOPL_PORT = 8082;
    protected final static int TWOPL_TM_PORT = 8083;
    protected final static String TWOPL_TM_IP = "127.0.0.1";
    protected final static String PERSISTENCE_ENGINE = ConfigStrings.PERSISTENCE_MEMORY;
    protected final static int SOCKET_TIMEOUT = 4000;
    protected final static String SERVER_BIND_IP = "127.0.0.1";
    protected final static String THEBES_TXN_MODE = ConfigStrings.HAT_MODE;
    protected final static Boolean TWOPL_REPLICATE_TO_SLAVES = true;
}