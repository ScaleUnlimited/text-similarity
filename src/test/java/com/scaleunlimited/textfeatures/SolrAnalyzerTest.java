package com.scaleunlimited.textfeatures;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.junit.Test;

import com.scaleunlimited.textfeatures.SolrAnalyzer;

public class SolrAnalyzerTest extends Assert {

    @Test
    public void test() throws Exception {
        SolrAnalyzer analyzer = new SolrAnalyzer();
        
        validateTerms(analyzer.getTermList("hello"), "hello");
        validateTerms(analyzer.getTermList("Hello world!"), "hello", "world");
        validateTerms(analyzer.getTermList("stemming tests"), "stem", "test");
        validateTerms(analyzer.getTermList("Bob's questions rock!"), "bob", "question", "rock");
        validateTerms(analyzer.getTermList("feature category"), "featur", "categori");
    }
    
    @Test
    public void testMultiWord() throws Exception {
        SolrAnalyzer analyzer = new SolrAnalyzer(3, new HashSet<String>());
        
        validateTerms(analyzer.getTermList("Hello there world!"), "hello", "there", "world", "hello there", "there world", "hello there world");
    }
    
    @Test
    public void testStopwords() throws Exception {
        Set<String> stopwords = new HashSet<String>();
        stopwords.add("there");
        SolrAnalyzer analyzer = new SolrAnalyzer(3, stopwords);
        
        validateTerms(analyzer.getTermList("Hello there world!"), "hello", "world");
    }
    
    private void validateTerms(List<String> actualTerms, String...expectedTerms) {
        // We have a set of terms that should be returned by the parser.
        // Verify the parser returns exactly that set.
        List<String> remainingTerms = new ArrayList<String>(expectedTerms.length);
        for (String term : expectedTerms) {
            remainingTerms.add(term);
        }
        
        for (String actualTerm : actualTerms) {
            boolean foundTerm = false;
            for (int i = 0; i < remainingTerms.size(); i++) {
                if (actualTerm.equals(remainingTerms.get(i))) {
                    remainingTerms.remove(i);
                    foundTerm = true;
                    break;
                }
            }
            
            assertTrue("Unexpected term returned by parser: " + actualTerm, foundTerm);
        }
        
        if (remainingTerms.size() > 0) {
            fail("One or more target terms not returned by parser: " + remainingTerms.toString());
        }
    }

    @Test
    public void testFilteringShortWords() throws Exception {
        SolrAnalyzer analyzer = new SolrAnalyzer(3, new HashSet<String>());
        
        // "my" will be filtered out, so we only get the two single terms.
        validateTerms(analyzer.getTermList("Hello my world!"), "hello", "world");
        
        validateTerms(analyzer.getTermList("my world"), "world");
        
        validateTerms(analyzer.getTermList("I am 1"));
    }
    
    @Test
    public void testFilteringNumbers() throws Exception {
        SolrAnalyzer analyzer = new SolrAnalyzer();
        
        // "my" will be filtered out, so we only get the two single terms.
        validateTerms(analyzer.getTermList("test 20 1.456 1,324"), "test");
    }
}
