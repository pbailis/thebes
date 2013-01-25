package edu.berkeley.thebes.common.config;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class CommandLineConfig {
    private static CommandLine commandLine;

    public static Options combineOptions(Options first, Options second) {
        final Options ret = new Options();
        for (Object option : first.getOptions())
            ret.addOption((Option) option);

        for (Object option : second.getOptions())
            ret.addOption((Option) option);

        return ret;
    }

    public static void initialize(Options options, String[] args) {
        try {
            commandLine = (new GnuParser()).parse(options, args);
        } catch (ParseException e) {
            System.err.println(
                    "Encountered exception while parsing using GnuParser:\n"
                    + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("thebes (server/client)", options);
        }
    }

    public static String getOption(String optionName) {
        return commandLine.getOptionValue(optionName);
    }
}