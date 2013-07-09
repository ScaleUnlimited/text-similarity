package com.scaleunlimited.cascading.snippets;

import org.junit.Test;

import cascading.flow.FlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Debug;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class TestLowercaseAll {

    private static class LowercaseAll extends BaseOperation implements Function {
        
        public LowercaseAll() {
            // We accept any number of arguments, and output all fields (whatever we get passed).
            super(ANY, Fields.ALL);
        }

        @Override
        public void operate(FlowProcess process, FunctionCall call) {
            // Get a copy of the incoming Tuple, which is unmodifiable.
            Tuple t = call.getArguments().getTupleCopy();
            
            for (int i = 0; i < t.size(); i++) {
                // This assumes every field value is (or can be cast to) a string.
                t.setString(i, t.getString(i).toLowerCase());
            }
            
            // Emit the lower-cased version of the incoming Tuple.
            call.getOutputCollector().add(t);
        }
    }
    
    @Test
    public void testLowercaseAll() throws Exception {
        Pipe pipe = new Pipe("in");
        pipe = new Each(pipe, new LowercaseAll());
        pipe = new Each(pipe, new Debug(true));
        
        InMemoryTap inTap = new InMemoryTap(new Fields("a", "b", "c"));
        TupleEntryCollector writer = inTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("ONE", "TWO", "THREE"));
        writer.close();

        new LocalFlowConnector().connect(inTap, new NullSinkTap(), pipe).complete();
    }
}
