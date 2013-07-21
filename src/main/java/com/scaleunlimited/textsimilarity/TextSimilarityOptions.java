/*
 * Copyright (c) 2013 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.scaleunlimited.textsimilarity;

import org.kohsuke.args4j.Option;

import com.scaleunlimited.cascading.BaseOptions;

public class TextSimilarityOptions extends BaseOptions {

    private String _input;
    private String _workingDir;
    private String _stopwords;
    private boolean _testMode = false;
    private int _maxTermsPerUser = 20;
    private int _shingleSize = 1;
    
    @Option(name = "-input", usage = "input data file or directory (emails in tsv format)", required = true)
    public void setInput(String input) {
        _input = input;
    }
  
    public String getInput() {
        return _input;
    }

    @Option(name = "-stopwords", usage = "input data file containing a list of stopwords", required = false)
    public void setStopwords(String stopwords) {
        _stopwords = stopwords;
    }
  
    public String getStopwords() {
        return _stopwords;
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

    @Option(name = "-maxterms", usage = "maximum number of terms to emit per user", required = false)
    public void setMaxTermsPerUser(int maxTermsPerUser) {
        _maxTermsPerUser = maxTermsPerUser;
    }
  
    public int getMaxTermsPerUser() {
        return _maxTermsPerUser;
    }

    @Option(name = "-shinglesize", usage = "maximum number of words per term", required = false)
    public void setShingleSize(int shingleSize) {
        _shingleSize = shingleSize;
    }
  
    public int getShingleSize() {
        return _shingleSize;
    }

    

}
