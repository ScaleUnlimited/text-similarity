package com.scaleunlimited.emailparsing;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.Property;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.local.LocalPlatform;
import com.scaleunlimited.cascading.local.TextLineScheme;

public class ParseEmailArchivesWorkflow {
    private static final Logger LOGGER = Logger.getLogger(ParseEmailArchivesWorkflow.class);
    
    /**
     * The RFC822 email parser from Tika can parse an mbox file, but you don't get separate
     * metadata for each email. We need that (author name, email address, etc) so we first
     * split the mbox file up into separate pieces, which we can then parse individually.
     *
     */
    @SuppressWarnings("serial")
    private static class MboxSplitterFunction extends BaseOperation<NullContext> implements Function<NullContext> {

        // Assume that each email begins with this text.
        private static final String MBOX_RECORD_DIVIDER = "From ";

        public MboxSplitterFunction() {
            // Output a single field which has all of the text for one email.
            super(new Fields("email"));
        }

        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            String filename = functionCall.getArguments().getString("line");
            Tuple result = new Tuple("");
            
            BufferedReader reader;
            try {
                InputStream stream = new FileInputStream(filename);
                reader = new BufferedReader(new InputStreamReader(stream, "us-ascii"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException("Impossible exception!", e);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Exception splitting mbox file %s", filename), e);
            }

            StringBuilder email = new StringBuilder();
            for (String curLine = safeReadLine(reader); curLine != null; curLine = safeReadLine(reader)) {
                if (curLine.startsWith(MBOX_RECORD_DIVIDER)) {
                    if (email.length() > 0) {
                        result.setString(0, email.toString());
                        functionCall.getOutputCollector().add(result);
                    }

                    email.setLength(0);
                }

                email.append(curLine);
                email.append('\n');
            }

            // Output the final record.
            if (email.length() > 0) {
                result.setString(0, email.toString());
                functionCall.getOutputCollector().add(result);
            }
        }
        
        private String safeReadLine(BufferedReader reader) {
            try {
                return reader.readLine();
            } catch (IOException e) {
                LOGGER.error("Unexpected exception while splitting mbox file", e);
                return null;
            }
        }
    }
    
    /**
     * Cascading Function that uses our customized RFC822 parser from Tika to parse
     * individual emails spit out by the MboxSplitterFunction.
     * 
     * Note that some email parsing will fail, because we don't include all of the
     * required Tika support for embedded content besides plain text.
     *
     */
    @SuppressWarnings("serial")
    private static class ParseEmail extends BaseOperation<NullContext> implements Function<NullContext> {

        private static final Pattern FULL_EMAIL_ADDRESS_PATTERN = Pattern.compile("(.*)<(.+@.+)>");
        private static final Pattern SIMPLE_EMAIL_ADDRESS_PATTERN = Pattern.compile("(.+@.+)");

        private transient Parser _parser;
        private transient ContentHandler _handler;
        private transient StringBuffer _content;
        
        private transient int _numEmails;
        private transient int _emailChars;
        private transient int _numSkipped;
        
        public ParseEmail() {
            // Fields that we'll emit in our resulting Tuple.
            super(new Fields("id", "author", "email", "subject", "date", "replyid", "content"));
        }
        
        @Override
        public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);

            _numEmails = 0;
            _emailChars = 0;
            _numSkipped = 0;
            
            _parser = new RFC822Parser();
            _content = new StringBuffer();

            _handler = new DefaultHandler() {
                private boolean inParagraph = false;
                private boolean inQuotes = false;

                @Override
                public void startDocument() throws SAXException {
                    super.startDocument();

                    inParagraph = false;
                    inQuotes = false;
                    _content.setLength(0);
                }

                @Override
                public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException {
                    if (localName.equalsIgnoreCase("p")) {
                        inParagraph = true;
                    } else if (localName.equalsIgnoreCase("q")) {
                        // FUTURE the RFC822 parser from Tika isn't adding quote elements to text, so
                        // currently this does nothing.
                        inQuotes = true;
                    } else if (localName.equalsIgnoreCase("br")) {
                        _content.append('\n');
                    } else if (localName.equalsIgnoreCase("meta")) {
                        // do nothing
                    }
                }

                @Override
                public void endElement(String uri, String localName, String name) throws SAXException {
                    if (localName.equalsIgnoreCase("p")) {
                        inParagraph = false;
                        _content.append('\n');
                    } else if (localName.equalsIgnoreCase("q")) {
                        inQuotes = false;
                    }
                }

                @Override
                public void characters(char[] ch, int start, int length) throws SAXException {
                    if (inParagraph && !inQuotes) {
                        // We have text we want to process.
                        _content.append(ch, start, length);
                        // HACK - parser isn't putting spaces or breaks between lines.
                        _content.append(' ');
                    }
                }
            };
        }

        
        @Override
        public void operate(FlowProcess flowProcess, FunctionCall<NullContext> functionCall) {
            String email = functionCall.getArguments().getString("email");
            _numEmails += 1;
            
            Metadata metadata = new Metadata();
                        
            try {
                InputStream stream = new ByteArrayInputStream(email.getBytes("UTF-8"));
                _parser.parse(stream, _handler, metadata, new ParseContext());

                // _content now has all of the body text, and metadata has the header info.
                String messageId = getMetadata(metadata, TikaCoreProperties.IDENTIFIER);

                String author = "";
                String address = "";
                String creator = getMetadata(metadata, TikaCoreProperties.CREATOR);
                Matcher addressMatcher = FULL_EMAIL_ADDRESS_PATTERN.matcher(creator);
                if (addressMatcher.matches()) {
                    author = addressMatcher.group(1);
                    address = addressMatcher.group(2);
                } else {
                    addressMatcher = SIMPLE_EMAIL_ADDRESS_PATTERN.matcher(creator);
                    if (addressMatcher.matches()) {
                        address = addressMatcher.group(1);
                    }
                }

                String subject = getMetadata(metadata, TikaCoreProperties.TITLE);
                String replyId = getMetadata(metadata, TikaCoreProperties.RELATION);
                String creationDate = getMetadata(metadata, TikaCoreProperties.CREATED);

                String content = _content.toString();
                _emailChars += content.length();

                // If size is greater than say 4x average, skip it. Otherwise we can get
                // some huge emails when a person includes all of the source code for their
                // project.
                if ((_numEmails > 100) && (content.length() > (4 *_emailChars / _numEmails))) {
                    _numSkipped += 1;
                    return;
                }

                // Need to convert all CRLF & raw linefeeds into \n sequences, so our file format is correct.
                // We do the same for tabs, so that it's easy to parse the result.
                content = content.replaceAll("\r\n", "\\\\n");
                content = content.replaceAll("[\r\n]", "\\\\n");
                content = content.replaceAll("\t", "\\\\t");

                Tuple tuple = new Tuple(messageId, author, address, subject, creationDate, replyId, content);
                functionCall.getOutputCollector().add(tuple);
            } catch (Exception e) {
                LOGGER.error("Exception parsing email: " + e.getMessage());
            } catch (NoClassDefFoundError e) {
                // This will happen when we have an embedded object (multi-part email) which
                // needs parsing support we don't include.
                LOGGER.error("Exception parsing email due to missing class: " + e.getMessage());
            }
        }
        
        private String getMetadata(Metadata metadata, Property property) {
            String result = metadata.get(property);
            if (result != null) {
                return result.trim();
            } else {
                return "";
            }
        }
        
        @Override
        public void cleanup(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
            System.out.println(String.format("Skipped %d emails out of %d", _numSkipped, _numEmails));
            super.cleanup(flowProcess, operationCall);
        }
    }
    

    /**
     * Create a Cascading Flow that will parse a set of mbox files and emit a tab-separated text
     * file with fields for the msgId, author, email address, etc.
     * 
     * Note this Flow will only run locally, since we're using the cascading.utils LocalPlatform.
     * 
     * @param options Settings for the flow
     * @return Flow suitable for execution
     * @throws Exception
     */
    public static Flow createFlow(ParseEmailArchivesOptions options) throws Exception {
        BasePlatform platform = new LocalPlatform(ParseEmailArchivesWorkflow.class);
        
        // We'll read individual file paths from the input file.
        BasePath inputPath = platform.makePath(options.getFileList());
        Tap sourceTap = platform.makeTap(platform.makeTextScheme(), inputPath);
        
        Pipe emailPipe = new Pipe("emails");
        emailPipe = new Each(emailPipe, new Fields("line"), new MboxSplitterFunction());
        emailPipe = new Each(emailPipe, new ParseEmail());
        
        BasePath outputPath = platform.makePath(options.getOutputDir());
        TextLineScheme scheme = new TextLineScheme(false);
        Tap sinkTap = platform.makeTap(scheme, outputPath, SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        Flow flow = flowConnector.connect(sourceTap, sinkTap, emailPipe);
        return flow;
    }
}
