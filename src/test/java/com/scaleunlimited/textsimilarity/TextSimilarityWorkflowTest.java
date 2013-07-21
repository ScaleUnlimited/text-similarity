package com.scaleunlimited.textsimilarity;

import junit.framework.Assert;

import org.junit.Test;

import cascading.flow.Flow;

public class TextSimilarityWorkflowTest extends Assert {

    @Test
    public void test() throws Exception {
        TextSimilarityOptions options = new TextSimilarityOptions();
        options.setTestMode(true);
        options.setInput("src/test/resources/mahout-emails.tsv");
        options.setWorkingDir("build/test/TextSimilarityWorkflowTest/test/working/");
        
        Flow<?> f = TextSimilarityWorkflow.createFlow(options);
        f.complete();
    }
    
    @Test
    public void testBigrams() throws Exception {
        TextSimilarityOptions options = new TextSimilarityOptions();
        options.setTestMode(true);
        options.setShingleSize(2);
        options.setInput("src/test/resources/mahout-emails-big.tsv");
        options.setWorkingDir("build/test/TextSimilarityWorkflowTest/testBigrams/working/");
        
        Flow<?> f = TextSimilarityWorkflow.createFlow(options);
        f.complete();
    }
    
    @Test
    public void testStopwords() throws Exception {
        TextSimilarityOptions options = new TextSimilarityOptions();
        options.setTestMode(true);
        options.setShingleSize(2);
        options.setStopwords("src/main/resources/stopwords.txt");
        options.setInput("src/test/resources/mahout-emails-big.tsv");
        options.setWorkingDir("build/test/TextSimilarityWorkflowTest/testStopwords/working/");
        
        Flow<?> f = TextSimilarityWorkflow.createFlow(options);
        f.complete();
    }
    
}
