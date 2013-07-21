/*
 * Copyright (c) 2013 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.scaleunlimited.textsimilarity;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.commons.io.IOUtils;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;
import com.scaleunlimited.cascading.local.LocalPlatform;
import com.scaleunlimited.cascading.ml.ITermsFilter;
import com.scaleunlimited.cascading.ml.ITermsParser;
import com.scaleunlimited.cascading.ml.TopTermsByLLR;
import com.scaleunlimited.textfeatures.Config;
import com.scaleunlimited.textfeatures.SolrAnalyzer;

public class TextSimilarityWorkflow {

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
    
    /**
     * Convert the "flattened" representation of emails (tab-separated values) into a Tuple
     * with the fields we care about (email address & content).
     *
     */
    @SuppressWarnings({"serial","rawtypes"})
    public static class StripQuotedText extends BaseOperation<NullContext> implements Function<NullContext> {

        public StripQuotedText() {
            super(1, Fields.ARGS);
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            String content = funcCall.getArguments().getTuple().getString(0);
            
            // Create an expression to strip out quoted text.
            content = content.replaceAll("(?m)^[>]{1,} .*", "");
            funcCall.getOutputCollector().add(new Tuple(content));
        }
    }
    
    @SuppressWarnings({"serial","rawtypes"})
    public static class StripQuoteHeader extends BaseOperation<NullContext> implements Function<NullContext> {
        
        public StripQuoteHeader() {
            super(1, Fields.ARGS);
        }
        
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            String content = funcCall.getArguments().getTuple().getString(0);
            
            // Create an expression to strip out the header for quoted text, which looks like:
            // > On Fri, May 31, 2013 at 4:00 PM, Ted Dun ning <ted.dunning@gmail.com> wrote:
            // On Tue, Jun 25, 2013 at 5:31 PM, Suneel Marthi <suneel_marthi@yahoo.com>wrote:

            content = content.replaceAll("(?m)^>* On ..., .+, .+wrote:", "");
            funcCall.getOutputCollector().add(new Tuple(content));
        }
    }
    
    @SuppressWarnings("serial")
    private static class TermsParser implements ITermsParser, Serializable {

        private String _text;
        private SolrAnalyzer _analyzer;
        
        public TermsParser(int shingleSize, String stopwordsFile) {
            try {
                Set<String> stopwords = getStopwords(stopwordsFile);
                _analyzer = new SolrAnalyzer(shingleSize, stopwords);
            } catch (Exception e) {
                throw new RuntimeException("Unable to create SolrAnalyzer for parsing text", e);
            }
        }
        
        @Override
        public Iterator<String> iterator() {
            List<String> termList = _analyzer.getTermList(_text);
            return termList.iterator();
        }

        @Override
        public void reset(String text) {
            _text = text;
        }

        private Set<String> getStopwords(String stopwordsFile) throws FileNotFoundException, IOException {
            Set<String> result = new HashSet<String>();
            if (stopwordsFile != null) {
                for (String stopword : IOUtils.readLines(new FileInputStream(stopwordsFile))) {
                    result.add(stopword);
                }
            }
            
            return result;
        }
        
        @Override
        public int getNumWords(String term) {
            int numWords = 1;
            int offset = 0;
            while ((offset = term.indexOf(' ', offset)) != -1) {
                numWords += 1;
            }
            
            return numWords;
        }
    }
    
    @SuppressWarnings("serial")
    private static class TermsFilter implements ITermsFilter, Serializable {

        private int _maxResults;
        
        public TermsFilter(int maxResults) {
            _maxResults = maxResults;
        }
        
        @Override
        public boolean filter(double llrScore, String term, ITermsParser parser) {
            return llrScore < 1.0;
        }

        @Override
        public int getMaxResults() {
            return _maxResults;
        }
        
    }
    
    /**
     * We have to convert a tuple that has "email", list of terms, list of scores
     * into N resulting Tuples (flattened), with one tuple for each email/term/score
     * combination.
     *
     */
    @SuppressWarnings({"serial","rawtypes"})
    public static class ParseLLRData extends BaseOperation<NullContext> implements Function<NullContext> {
        
        ParseLLRData() {
            super( new Fields(Config.EMAIL_FN, "term", "score") );
        }
        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> funcCall) {
            TupleEntry te = funcCall.getArguments();
            String email = te.getString(Config.EMAIL_FN);
            
            // Terms and scores are parallel arrays of values (strings and doubles)
            Tuple terms = (Tuple)funcCall.getArguments().getObject("terms");
            Tuple scores = (Tuple)funcCall.getArguments().getObject("scores");
            
            TupleEntryCollector outputCollector = funcCall.getOutputCollector();
            int numTerms = terms.size();
            for (int i = 0; i < numTerms; i++) {
                outputCollector.add(new Tuple(email, terms.getString(i), scores.getDouble(i)) );
            }
        }
    }

    
    @SuppressWarnings("rawtypes")
    public static Flow createFlow(TextSimilarityOptions options) throws Exception {
        
        BasePlatform platform  = options.isTestMode() ? new LocalPlatform(TextSimilarityWorkflow.class) 
                                                      : new HadoopPlatform(TextSimilarityWorkflow.class);
        
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        
        BasePath inputPath = platform.makePath(options.getInput());
        platform.assertPathExists(inputPath, "input file");
        Tap inputSource = platform.makeTap(platform.makeTextScheme(), inputPath);
        Pipe inputPipe = new Pipe("input");
        
        // Parse the input file to extract the email and the text content fields
        inputPipe = new Each(inputPipe, new ParseEmails());
        
        // Remove quoted text
        // inputPipe = new Each(inputPipe, new Fields(CONTENT_FN), new StripQuotedText(), Fields.REPLACE);
        
        // Remove quote header
        inputPipe = new Each(inputPipe, new Fields(Config.CONTENT_FN), new StripQuoteHeader(), Fields.REPLACE);

        // Now use the TopTermsByLLR SubAssembly to extract N top terms
        final int mapSideCacheSize = 10000;
        Pipe termsPipe = new TopTermsByLLR(inputPipe,
                                           new TermsParser(options.getShingleSize(), options.getStopwords()),
                                           new TermsFilter(options.getMaxTermsPerUser()), 
                                           new Fields(Config.EMAIL_FN),
                                           new Fields(Config.CONTENT_FN),
                                           mapSideCacheSize);
        
        // We need to emit one line per email/term combination (with scores).
        termsPipe = new Each(termsPipe, new ParseLLRData());
        
        // Sort by user, then score
        Fields groupFields = new Fields(Config.EMAIL_FN, "score");
        termsPipe = new GroupBy(termsPipe, groupFields, true);
        
        BasePath termsPath = platform.makePath(workingDirPath, Config.TERMS_DIR);
        Tap termsSink = platform.makeTap(platform.makeTextScheme(), termsPath, SinkMode.REPLACE);

        FlowDef flowDef = new FlowDef();
        flowDef.addSource(inputPipe, inputSource);
        flowDef.addTailSink(termsPipe, termsSink);
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(flowDef);
    }
    
    
}
