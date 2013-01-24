package edu.berkeley.thebes.common.config;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Map;

public class YamlConfig {

    public Map<String, Object> getConfig(String configFile) throws FileNotFoundException {
        return (Map<String, Object>) (new Yaml()).load(new FileInputStream(new File(configFile)));
    }
}