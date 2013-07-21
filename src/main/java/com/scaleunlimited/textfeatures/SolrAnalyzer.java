/*
 * Copyright (c) 2013 Scale Unlimited
 * 
 * All rights reserved.
 */

package com.scaleunlimited.textfeatures;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.solr.core.CoreContainer;
import org.apache.solr.core.SolrCore;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.xml.sax.SAXException;

@SuppressWarnings("serial")
public class SolrAnalyzer implements Serializable {

    private static final int DEFAULT_SHINGLE_SIZE = 1;

    private static final int MIN_WORD_LENGTH = 3;
    
    private int _shingleSize;
    private Set<String> _stopwords;
    
    private transient Analyzer _analyzer;
    private transient ReusableStringReader _stringReader;
    private transient ThreadLocal<TokenStream> _tokenStream;
    private transient ThreadLocal<CharTermAttribute> _token;

    public SolrAnalyzer() throws IOException, ParserConfigurationException, SAXException {
        this(DEFAULT_SHINGLE_SIZE, new HashSet<String>());
    }

    public SolrAnalyzer(int shingleSize, Set<String> stopwords) throws IOException, ParserConfigurationException, SAXException {
        _shingleSize = shingleSize;
        _stopwords = stopwords;
    }
    
    private synchronized void init() {
        if (_analyzer == null) {
            _stringReader = new ReusableStringReader("");
            _tokenStream = new ThreadLocal<TokenStream>();
            _token = new ThreadLocal<CharTermAttribute>();

            try {
                _analyzer = getAnalyzer("text_en");
            } catch (Exception e) {
                throw new RuntimeException("Can't creating Solr-based analyzer", e);
            }
        }
    }
    
    public List<String> getTermList(String contentText) {
        init();
        
        List<String> result = new ArrayList<String>(contentText.length() / 10);
        
        try {
            _stringReader.reset(contentText);
            _tokenStream.set(_analyzer.tokenStream("content", _stringReader));
            
            if (_token.get() == null) {
                _token.set((CharTermAttribute)_tokenStream.get().addAttribute(CharTermAttribute.class));
            }
            
            _tokenStream.get().reset();
            _tokenStream.get().incrementToken();
            
            String[] wordQueue = new String[_shingleSize];
            StringBuffer term = new StringBuffer();
            while (_token.get().length() > 0) {
                String curWord = new String(_token.get().buffer(), 0, _token.get().length());
                curWord = filterWord(curWord);
                for (int i = wordQueue.length - 1; i > 0; i--) {
                    wordQueue[i] = wordQueue[i - 1];
                }
                wordQueue[0] = curWord;
                
                // Now add all of the terms from the shingled results.
                term.delete(0, term.length());
                for (int i = 0; i < wordQueue.length; i++) {
                    if (wordQueue[i] == null) {
                        break;
                    }
                    
                    if (i > 0) {
                        term.insert(0, ' ');
                    }
                    
                    term.insert(0, wordQueue[i]);
                    result.add(term.toString());
                }

                _tokenStream.get().incrementToken();
            }
        } catch (IOException e) {
            throw new RuntimeException("Impossible error", e);
        }

        return result;
    }

    private String filterWord(String curWord) {
        // Check word length
        if (curWord.length() < MIN_WORD_LENGTH) {
            return null;
        }
        
        // Check if it's a number.
        boolean isNumber = true;
        for (int i = 0; isNumber && (i < curWord.length()); i++) {
            char c = curWord.charAt(i);
            isNumber = Character.isDigit(c) || (c == ',') || (c == '.');
        }
        
        if (isNumber) {
            return null;
        }
        
        // Check if it's in our stopwords list.
        if (_stopwords.contains(curWord)) {
            return null;
        }
        
        return curWord;
    }

    /**
     * Leverage the Solr schema.xml analysis chain to get the right analyzer for the target language.
     * 
     * @param solrCoreDirName
     * @param language target language
     * @param modifier field name modifier (e.g. "_raw" for no stemming or special splitting).
     * @return
     * @throws IOException
     * @throws ParserConfigurationException
     * @throws SAXException
     */
    private Analyzer getAnalyzer(String fieldTypeName) throws IOException, ParserConfigurationException, SAXException {
        // Create a temp location for Solr home, which has a skeleton solr.xml that
        // references the Solr core directory.
        File tmpSolrHome = new File(FileUtils.getTempDirectory(), UUID.randomUUID().toString());
        File solrCoreDir = makeSolrCoreDir(tmpSolrHome);
        String coreName = solrCoreDir.getName();
        String corePath = solrCoreDir.getAbsolutePath();
        String solrXmlContent = String.format("<solr><cores><core name=\"%s\" instanceDir=\"%s\"></core></cores></solr>",
                                              coreName, corePath);
        File solrXmlFile = new File(tmpSolrHome, "solr.xml");
        FileUtils.write(solrXmlFile, solrXmlContent);
        
        System.setProperty("solr.solr.home", tmpSolrHome.getAbsolutePath());
        CoreContainer.Initializer initializer = new CoreContainer.Initializer();
        CoreContainer coreContainer = null;
        
        try {
            coreContainer = initializer.initialize();
            Collection<SolrCore> cores = coreContainer.getCores();
            SolrCore core = null;
            
            if (cores.size() == 0) {
                throw new IllegalArgumentException("No Solr cores are available");
            } else if (cores.size() == 1) {
                core = cores.iterator().next();
            } else {
                throw new IllegalArgumentException("Only one Solr core is supported");
            }
            
            IndexSchema schema = core.getSchema();
            FieldType fieldType = schema.getFieldTypeByName(fieldTypeName);
            if (fieldType == null) {
                throw new IllegalArgumentException(String.format("No analyzer found for field type \"%s\"", fieldTypeName));
            }

            // Get the analyzer - this will be either the single analyzer defined, or the index analyzer (which is generally
            // what we want) if a separate index vs. query analyzer has been defined.
            return fieldType.getAnalyzer();
        } finally {
            if (coreContainer != null) {
                coreContainer.shutdown();
            }
        }
    }

    private File makeSolrCoreDir(File solrHomeDir) throws IOException {
        List<String> filenames = IOUtils.readLines(SolrAnalyzer.class.getResourceAsStream("/solrparser/filelist.txt"));
        
        File containerDir = new File(solrHomeDir, "solrparser");
        File confDir = new File(containerDir, "conf");
        confDir.mkdirs();
        
        for (String filename : filenames) {
            filename = filename.trim();
            if (filename.isEmpty() || filename.startsWith("#")) {
                continue;
            }
            
            File dstFile = new File(confDir, filename);
            OutputStream os = new FileOutputStream(dstFile);
            InputStream is = SolrAnalyzer.class.getResourceAsStream("/solrparser/" + filename);
            IOUtils.copy(is, os);
            os.close();
            is.close();
        }
        
        return containerDir;
    }
    

}

