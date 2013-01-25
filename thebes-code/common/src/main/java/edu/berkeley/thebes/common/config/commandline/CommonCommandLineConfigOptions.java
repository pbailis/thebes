package edu.berkeley.thebes.common.config.commandline;

import edu.berkeley.thebes.common.config.ConfigStrings;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

public class CommonCommandLineConfigOptions {
    public static Options constructCommonOptions() {
        final Options commonOptions = new Options();
        commonOptions.addOption(new Option(ConfigStrings.CONFIG_FILE, true, "configuration file"));
        Option clusterIdOption = new Option(ConfigStrings.CLUSTER_ID, true, "cluster ID");
        clusterIdOption.setRequired(true);
        commonOptions.addOption(clusterIdOption);

        return commonOptions;
    }
}