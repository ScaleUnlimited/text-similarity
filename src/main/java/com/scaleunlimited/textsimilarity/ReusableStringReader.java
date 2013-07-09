package com.scaleunlimited.textsimilarity;

import java.io.IOException;
import java.io.Reader;

public class ReusableStringReader extends Reader {

    private char[] _chars;
    private int _offset;
    
    public ReusableStringReader(String s) {
        reset(s);
    }
    
    public void reset(String s) {
        _chars = s.toCharArray();
        _offset = 0;
    }
    
    @Override
    public void reset() throws IOException {
        _offset = 0;
    }
    
    @Override
    public void close() throws IOException {
        _chars = null;
        _offset = 0;
    }

    /* Put up to <len> characters into <cbuf> at <offset>
     * (non-Javadoc)
     * @see java.io.Reader#read(char[], int, int)
     */
    @Override
    public int read(char[] cbuf, int offset, int len) throws IOException {
        if (_offset >= _chars.length) {
            return -1;
        }
        
        int numChars = Math.min(len, _chars.length - _offset);
        System.arraycopy(_chars, _offset, cbuf, offset, numChars);
        _offset += numChars;
        
        return numChars;
    }
    
}
