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
        options.setShingleSize(2);
        
        Flow<?> f = TextSimilarityWorkflow.createFlow(options);
        f.complete();
    }
    
}
