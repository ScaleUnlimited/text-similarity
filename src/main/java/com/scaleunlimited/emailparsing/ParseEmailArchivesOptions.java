package com.scaleunlimited.emailparsing;

import org.kohsuke.args4j.Option;

import com.scaleunlimited.cascading.BaseOptions;

public class ParseEmailArchivesOptions extends BaseOptions {

    private String _fileList;
    private String _outputDir;

    @Option(name = "-filelist", usage = "input file with list of paths to mbox (email archive) files", required = true)
    public void setFileList(String fileList) {
        _fileList = fileList;
    }
    
    public String getFileList() {
        return _fileList;
    }

    @Option(name = "-outputdir", usage = "output dir for textual representation of emails", required = true)
    public void setOutputDir(String output) {
        _outputDir = output;
    }
    
    public String getOutputDir() {
        return _outputDir;
    }

}
