package edu.berkeley.thebes.common.config.commandline;

import edu.berkeley.thebes.common.config.ConfigStrings;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class ClientCommandLineConfigOptions {
    public static Options constructClientOptions() {
        final Options clientOptions = new Options();
        clientOptions.addOption(new Option(ConfigStrings.TXN_MODE, true, "hat type"));

        return clientOptions;
    }
}