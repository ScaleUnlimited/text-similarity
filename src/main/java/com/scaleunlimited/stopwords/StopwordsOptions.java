/*
 * Copyright (c) 2013 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.scaleunlimited.stopwords;

import org.kohsuke.args4j.Option;

import com.scaleunlimited.cascading.BaseOptions;

public class StopwordsOptions extends BaseOptions {

    public static float NO_MAX_DF = 1.0f;
    
    private String _input;
    private String _workingDir;
    private boolean _testMode = false;
    private float _maxDocumentFrequency = NO_MAX_DF;
    
    @Option(name = "-input", usage = "input data file (emails in tsv format)", required = true)
    public void setInput(String input) {
        _input = input;
    }
  
    public String getInput() {
        return _input;
    }

    @Option(name = "-workingdir", usage = "working directory", required = true)
    public void setWorkingDir(String workingDir) {
        _workingDir = workingDir;
    }
  
    public String getWorkingDir() {
        return _workingDir;
    }
    
    @Option(name = "-testmode", usage = "run locally using Cascading local mode", required = false)
    public void setTestMode(boolean testMode) {
        _testMode = testMode;
    }
  
    public boolean isTestMode() {
        return _testMode;
    }

    @Option(name = "-maxdf", usage = "maximum document frequency for good terms", required = false)
    public void setMaxDocumentFrequency(float maxDocumentFrequency) {
        _maxDocumentFrequency = maxDocumentFrequency;
    }
  
    public float getMaxDocumentFrequency() {
        return _maxDocumentFrequency;
    }

}
