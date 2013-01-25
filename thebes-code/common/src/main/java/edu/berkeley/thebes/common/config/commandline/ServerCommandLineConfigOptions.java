package edu.berkeley.thebes.common.config.commandline;

import edu.berkeley.thebes.common.config.ConfigStrings;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ServerCommandLineConfigOptions {
    public static Options constructServerOptions() {
        final Options serverOptions = new Options();
        serverOptions.addOption(new Option(ConfigStrings.SERVER_PORT, true, "server port"));

        Option clusterIdOption = new Option(ConfigStrings.SERVER_ID, true, "cluster ID");
        clusterIdOption.setRequired(true);
        serverOptions.addOption(clusterIdOption);

        serverOptions.addOption(new Option(ConfigStrings.SERVER_BIND_IP, true, "server bind ip address"));
        serverOptions.addOption(new Option(ConfigStrings.STANDALONE_MODE, false, "standalone server mode flag"));

        return serverOptions;
    }
}