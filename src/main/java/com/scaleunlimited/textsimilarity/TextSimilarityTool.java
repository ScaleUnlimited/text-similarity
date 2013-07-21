package com.scaleunlimited.textsimilarity;

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import cascading.flow.Flow;

import com.scaleunlimited.cascading.BaseTool;

/**
 * Simple tool to build a workflow to run the TextSimilarityWorkflow
 * on email data
 */
public class TextSimilarityTool extends BaseTool {

    private static void error(String message, CmdLineParser parser) {
        System.err.println(message);
        printUsageAndExit(parser);
    }


    public static void main(String[] args) {
        TextSimilarityOptions options = new TextSimilarityOptions();
        CmdLineParser parser = new CmdLineParser(options);

        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            error(e.getMessage(), parser);
        }
        
        try {
            Flow<?> flow = TextSimilarityWorkflow.createFlow(options);
            
            if (options.getDOTFile() != null) {
                flow.writeDOT(options.getDOTFile());
            }
            
            flow.complete();
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }
    }

}
