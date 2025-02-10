package org.raj.sample;


import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.HelpFormatter;

// A util class to Parse command line arguments for the job
public class CommandLineParserUtil {

    public static CommandLine parseArguments(String[] args) {
        Options options = new Options();
        options.addOption("i", "input", true, "Input path");
        options.addOption("o", "output", true, "Output path");
        options.addOption("t", "timeBucket", true, "Time bucket like '30 minutes', '1 hour', '24 hours', etc. ");

        CommandLineParser parser = new DefaultParser();
        try {
            return parser.parse(options, args);
        } catch (ParseException e) {
            System.err.println("Error parsing command-line arguments: " + e.getMessage());
            new HelpFormatter().printHelp("TimeSeriesAggregator", options);
            throw new RuntimeException(e);
        }
    }

}
