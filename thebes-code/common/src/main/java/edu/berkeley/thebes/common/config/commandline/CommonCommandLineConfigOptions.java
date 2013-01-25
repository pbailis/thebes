package edu.berkeley.thebes.common.config.commandline;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CommonCommandLineConfigOptions {
    public static Options constructCommonOptions() {
        final Options commonOptions = new Options();
        commonOptions.addOption(new Option("c", "config", true, "configuration file"));

        return commonOptions;
    }
}