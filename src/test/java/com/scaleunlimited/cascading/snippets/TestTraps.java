package com.scaleunlimited.cascading.snippets;

import org.junit.Test;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.pipe.Pipe;
import cascading.scheme.hadoop.TextDelimited;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

public class TestTraps {

    @Test
    public void testTrappingTapException() throws Exception {
        final Fields testFields = new Fields("user", "value");
        
        String inDir = "build/test/TrapTest/testTrappingTapException/in";
        String outDir = "build/test/TrapTest/testTrappingTapException/out";
        String trapDir = "build/test/TrapTest/testTrappingTapException/trap";

        Tap writeTap = new Hfs(new TextLine(), inDir, SinkMode.REPLACE);
        TupleEntryCollector write = writeTap.openForWrite(new HadoopFlowProcess());
        write.add(new Tuple("user1", 1));
        write.add(new Tuple("user2", 0));
        write.add(new Tuple("user3"));
        write.close();

        Tap sourceTap = new Hfs(new TextDelimited(testFields), inDir);
        
        Pipe pipe = new Pipe("test");
        
        Tap trapTap = new Hfs(new TextLine(), trapDir, SinkMode.REPLACE);
        Tap sinkTap = new Hfs(new TextLine(), outDir, SinkMode.REPLACE);
        
        FlowDef flowDef = new FlowDef();
        flowDef.addSource(pipe, sourceTap);
        flowDef.addTailSink(pipe, sinkTap);
        flowDef.addTrap(pipe, trapTap);
        
        Flow flow = new HadoopFlowConnector().connect(flowDef);
        flow.complete();
    }


}
