package com.scaleunlimited.emailparsing;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import cascading.flow.Flow;

import com.scaleunlimited.cascading.BaseTool;

/**
 * Simple tool to build a workflow to parse mbox files and generate
 * tab-separated output files with fields for interesting data from
 * each email (msgId, author name, email address, etc)
 *
 */
public class ParseEmailArchivesTool extends BaseTool {

    private static void error(String message, CmdLineParser parser) {
        System.err.println(message);
        printUsageAndExit(parser);
    }


    public static void main(String[] args) {
        ParseEmailArchivesOptions options = new ParseEmailArchivesOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            error(e.getMessage(), parser);
        }
        
        try {
            Flow flow = ParseEmailArchivesWorkflow.createFlow(options);
            flow.complete();
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
