package edu.berkeley.thebes.common.config;

import org.yaml.snakeyaml.Yaml;

import javax.naming.ConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

public class YamlConfig {
    private static Map<String, Object> config;

    protected static void initialize(String configFile) throws FileNotFoundException, ConfigurationException {
        if (configFile == null)
            configFile = ConfigDefaults.CONFIG_LOCATION;

        config = (Map<String, Object>) (new Yaml()).load(new FileInputStream(new File(configFile)));
        if (config == null)
            config = new HashMap<String, Object>();
    }

    protected static Object getOption(String optionName) {
        return config.get(optionName);
    }
}