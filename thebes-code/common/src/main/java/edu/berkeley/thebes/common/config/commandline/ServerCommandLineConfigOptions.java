package edu.berkeley.thebes.common.config.commandline;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ServerCommandLineConfigOptions {
    public static Options constructServerOptions() {
        final Options serverOptions = new Options();
        serverOptions.addOption(new Option("p", "port", true, "server port"));

        return serverOptions;
    }
}