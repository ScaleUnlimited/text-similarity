/*
 * Copyright (c) 2013 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.scaleunlimited.stopwords;

import java.util.List;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.Identity;
import cascading.operation.Insert;
import cascading.operation.OperationCall;
import cascading.operation.expression.ExpressionFilter;
import cascading.operation.expression.ExpressionFunction;
import cascading.operation.filter.Limit;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.CountBy;
import cascading.pipe.assembly.Unique;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.UniqueCount;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;
import com.scaleunlimited.textfeatures.Config;
import com.scaleunlimited.textfeatures.SolrAnalyzer;

public class StopwordsWorkflow {


    /**
     * Convert the "flattened" representation of emails (tab-separated values) into a Tuple
     * with the fields we care about (email address & content).
     *
     */
    @SuppressWarnings({"serial","rawtypes"})
    public static class ParseEmails extends BaseOperation<NullContext> implements Function<NullContext> {

        public ParseEmails() {
            super(new Fields(Config.EMAIL_FN, Config.CONTENT_FN));
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
 
            // We have a line of text with a bunch of tab-separated fields.
            // msgId, author, email, subject, date, replyId, content
            String inputLine = funcCall.getArguments().getString("line");
            String[] fields = inputLine.split("\t", -1);
            if (fields.length != 7) {
                // bad data, throw an exception
                throw new RuntimeException("Invalid input line: " + inputLine);
            }

            // Normalize the email address
            String email = fields[2].trim().toLowerCase();
            
            // Content comes in with newlines and tabs escaped, so un-escape them now.
            String content = fields[6].replaceAll("\\\\n", "\n").replaceAll("\\\t", "\t");
            funcCall.getOutputCollector().add(new Tuple(email, content));
        }
    }
    
    @SuppressWarnings({"serial","rawtypes"})
    public static class ParseText extends BaseOperation<NullContext> implements Function<NullContext> {

        private transient SolrAnalyzer _analyzer;
        private transient Tuple _result;
        
        public ParseText() {
            super(new Fields(Config.TERM_FN));
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            
            _result = Tuple.size(1);
            
            try {
                _analyzer = new SolrAnalyzer();
            } catch (Exception e) {
                throw new RuntimeException("Unable to create SolrAnalyzer for parsing text", e);
            }
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            TupleEntry te = functionCall.getArguments();
            List<String> termList = _analyzer.getTermList(te.getString(Config.CONTENT_FN));

            for (String term : termList) {
                _result.setString(0, term);
                functionCall.getOutputCollector().add(_result);
            }
        }
    }
    
    @SuppressWarnings("rawtypes")
    public static Flow createFlow(StopwordsOptions options) throws Exception {
        BasePlatform platform  = options.isTestMode() ? new LocalPlatform(StopwordsWorkflow.class) 
                                                      : new HadoopPlatform(StopwordsWorkflow.class);
        
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        
        BasePath inputPath = platform.makePath(options.getInput());
        platform.assertPathExists(inputPath, "input file");
        Tap inputSource = platform.makeTap(platform.makeTextScheme(), inputPath);
        Pipe inputPipe = new Pipe("input");
        
        // Parse the input file to extract the email and the text content fields
        inputPipe = new Each(inputPipe, new ParseEmails());
        
        // Parse the text to extract terms
        Pipe termsPipe = new Pipe("terms", inputPipe);
        termsPipe = new Each(termsPipe, new Fields(Config.CONTENT_FN), new ParseText(), Fields.SWAP);
        
        // For each term, count how many different users (email addresses) contain the term
        termsPipe = new UniqueCount(termsPipe, new Fields(Config.TERM_FN), new Fields(Config.EMAIL_FN), new Fields(Config.DOC_COUNT_FN));

        // Find out the total number of documents. First get rid of everything but the one
        // field that we need. Then do a unique on the email address, and count occurrences.
        Pipe docsPipe = new Pipe("docs", inputPipe);
        docsPipe = new Each(inputPipe, new Fields(Config.EMAIL_FN), new Identity(new Fields("doc_email")));
        docsPipe = new Unique(docsPipe, new Fields("doc_email"));
        docsPipe = new Each(docsPipe, new Insert(new Fields("doc_constant"), 1), Fields.ALL);
        docsPipe = new CountBy(docsPipe, new Fields("doc_constant"), new Fields(Config.TOTAL_DOCS_FN));

        // Now we need to add a field to each pipe that we can use for joining,
        // which is kind of lame but the only way currently.
        termsPipe = new Each(termsPipe, new Insert(new Fields("term_constant"), 1), Fields.ALL);
        
        // Now we can HashJoin the term + doc count pipe with the total doc count pipe.
        Pipe dfPipe = new HashJoin( termsPipe, new Fields("term_constant"),
                                    docsPipe, new Fields("doc_constant"));
        
        dfPipe = new Each(dfPipe, new Fields(Config.TERM_FN, Config.DOC_COUNT_FN, Config.TOTAL_DOCS_FN), new Identity());
        String dfCalc = String.format("(float)%s / (float)%s", Config.DOC_COUNT_FN, Config.TOTAL_DOCS_FN);
        dfPipe = new Each(  dfPipe,
                            new Fields(Config.DOC_COUNT_FN, Config.TOTAL_DOCS_FN),
                            new ExpressionFunction(new Fields("df"), dfCalc, Float.class),
                            Fields.SWAP);
        
        // Sort by document frequency (df), from high to low
        Fields groupFields = new Fields("df");
        dfPipe = new GroupBy(dfPipe, groupFields, true);
        
        // Now figure out if we're generating a list of all terms for analysis, or just a "bad words" list
        // with the top 2000 terms that have a DF > the max DF. If so then filter out everything is isn't a bad word.
        if (options.getMaxDocumentFrequency() != StopwordsOptions.NO_MAX_DF) {
            String dfFilter = String.format("df <= %f", options.getMaxDocumentFrequency());
            dfPipe = new Each(dfPipe, new Fields("df"), new ExpressionFilter(dfFilter, Float.class));
        } else {
            // Limit to top 2000 results.
            dfPipe = new Each(dfPipe, new Limit(2000));
        }
        
        BasePath termsPath = platform.makePath(workingDirPath, Config.TERMS_BY_DF_DIR);
        Tap termsSink = platform.makeTap(platform.makeTextScheme(), termsPath, SinkMode.REPLACE);

        FlowDef flowDef = new FlowDef();
        flowDef.addSource(inputPipe, inputSource);
        flowDef.addTailSink(dfPipe, termsSink);
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(flowDef);
    }
    
    
}
