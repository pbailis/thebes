package edu.berkeley.thebes.common.config;

/**
 * Special type interface between the config YAML and Java.
 * If a ConfigParameter requires a ConfigParameterType, you should use the lowercase
 * value (e.g., "twopl" to set TWOPL for TransactionMode).
 */
public interface ConfigParameterTypes {
    public enum PersistenceEngine implements ConfigParameterTypes {
        MEMORY;
    }
    
    public enum TransactionMode implements ConfigParameterTypes {
        HAT (ConfigParameters.CLUSTER_CONFIG),
        TWOPL (ConfigParameters.TWOPL_CLUSTER_CONFIG);
        
        private final ConfigParameters clusterConfigParam;

        private TransactionMode(ConfigParameters clusterConfigParam) {
            this.clusterConfigParam = clusterConfigParam;
        }

        public ConfigParameters getClusterConfigParam() {
            return clusterConfigParam;
        }
    }
}