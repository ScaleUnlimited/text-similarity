package com.scaleunlimited.stopwords;

import static org.junit.Assert.*;

import org.junit.Test;

import cascading.flow.Flow;

import com.scaleunlimited.textsimilarity.TextSimilarityOptions;
import com.scaleunlimited.textsimilarity.TextSimilarityWorkflow;

public class StopwordsWorkflowTest {

    @Test
    public void test() throws Exception {
        StopwordsOptions options = new StopwordsOptions();
        options.setTestMode(true);
        options.setInput("src/test/resources/mahout-emails-big.tsv");
        options.setWorkingDir("build/test/StopwordsWorkflowTest/test/working/");
        
        Flow<?> f = StopwordsWorkflow.createFlow(options);
        f.complete();
    }

    @Test
    public void testBadWordsGeneration() throws Exception {
        StopwordsOptions options = new StopwordsOptions();
        options.setTestMode(true);
        options.setMaxDocumentFrequency(0.20f);
        options.setInput("src/test/resources/mahout-emails-big.tsv");
        options.setWorkingDir("build/test/StopwordsWorkflowTest/testBadWordsGeneration/working/");
        
        Flow<?> f = StopwordsWorkflow.createFlow(options);
        f.complete();
    }

}
