package com.scaleunlimited.textsimilarity;

import java.util.Iterator;

import junit.framework.Assert;

import org.junit.Test;

import cascading.CascadingTestCase;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleListCollector;

@SuppressWarnings("serial")
public class TextSimilarityOperationsTest extends CascadingTestCase {

    @Test
    public void testStrippingQuotedText() {
        TupleListCollector collector = invokeFunction(new TextSimilarityWorkflow.StripQuotedText(), new Tuple("word1\n> word2\n>> word3\nword4"), new Fields("content"));
        Iterator<Tuple> iter = collector.iterator();
        Assert.assertTrue(iter.hasNext());
        
        String result = iter.next().getString(0);
        Assert.assertTrue(result.contains("word1"));
        Assert.assertFalse(result.contains("word2"));
        Assert.assertFalse(result.contains("word3"));
        Assert.assertTrue(result.contains("word4"));
        Assert.assertFalse(iter.hasNext());
    }
    
    @Test
    public void testStripQuoteHeader() {
        String content = "line1\r On Fri, May 31, 2013 at 4:00 PM, Ted Dun ning <ted.dunning@gmail.com> wrote:";
        TupleListCollector collector = invokeFunction(new TextSimilarityWorkflow.StripQuoteHeader(), new Tuple(content), new Fields("content"));
        Iterator<Tuple> iter = collector.iterator();
        Assert.assertTrue(iter.hasNext());
        
        String result = iter.next().getString(0);
        Assert.assertTrue(result.contains("line1"));
        Assert.assertFalse(result.contains("gmail.com"));
        Assert.assertFalse(iter.hasNext());
    }
}
