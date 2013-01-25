package edu.berkeley.thebes.common.config;

import edu.berkeley.thebes.common.config.commandline.ClientCommandLineConfigOptions;
import edu.berkeley.thebes.common.config.commandline.CommonCommandLineConfigOptions;
import edu.berkeley.thebes.common.config.commandline.ServerCommandLineConfigOptions;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;

public class Config {
    public static void initializeClientConfig(String[] commandLine) throws FileNotFoundException,
                                                                           ConfigurationException {
        CommandLineConfig.initialize(CommandLineConfig.combineOptions(
                ClientCommandLineConfigOptions.constructClientOptions(),
                CommonCommandLineConfigOptions.constructCommonOptions()),
                                     commandLine);
        YamlConfig.initialize(CommandLineConfig.getOption("config"));
    }

    public static void initializeServerConfig(String[] commandLine)
            throws FileNotFoundException, ConfigurationException {
        CommandLineConfig.initialize(CommandLineConfig.combineOptions(
                ServerCommandLineConfigOptions.constructServerOptions(),
                CommonCommandLineConfigOptions.constructCommonOptions()),
                                     commandLine);
        YamlConfig.initialize(CommandLineConfig.getOption("config"));
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
}