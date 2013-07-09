package com.scaleunlimited.cascading.snippets;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.flow.local.LocalFlowConnector;
import cascading.flow.local.LocalFlowProcess;
import cascading.operation.Debug;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.expression.ExpressionFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.HashJoin;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.SumBy;
import cascading.pipe.joiner.InnerJoin;
import cascading.pipe.joiner.LeftJoin;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.NullSinkTap;
import com.scaleunlimited.cascading.local.InMemoryTap;

public class TestHashJoin extends Assert {

    @Test
    public void testHashJoin() throws Exception {
        Pipe siteInfo = new Pipe("site info");
        siteInfo = new Each(siteInfo, new ExpressionFilter("siteid != 823", Integer.TYPE));
        siteInfo = new Each(siteInfo, new Debug("site-info", true));
        
        Pipe parsed = new Pipe("parsed");
        parsed = new Each(parsed, new ExpressionFilter("sid != 823", Integer.TYPE));
        parsed = new Each(parsed, new Debug("filterd-profile", true));
        
        //Join the two pipes
        Pipe joinPipe = new HashJoin(parsed, new Fields("sid"),
                siteInfo, new Fields("siteid"),  new LeftJoin());
        joinPipe = new Each(joinPipe, new Debug("join siteId",  true));

        InMemoryTap siteTap = new InMemoryTap(new Fields("siteid", "partnerid"));
        TupleEntryCollector writer = siteTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple(823, 205));
        writer.add(new Tuple(100, 32));
        writer.close();
        
        InMemoryTap parsedTap = new InMemoryTap(new Fields("uuid", "catid", "sid"));
        writer = parsedTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("+9L991nCHa9J/R8i", 22599, 823));
        writer.add(new Tuple("+8J881nCHa9J/R8i", 99522, 328));
        writer.close();
        
        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(siteInfo.getName(), siteTap);
        sources.put(parsed.getName(), parsedTap);
        
        new LocalFlowConnector().connect(sources, new NullSinkTap(), joinPipe).complete();
    }

    @Test
    public void testNoAggregationAfterHashJoin() throws Exception {
        Pipe lhsPipe = new Pipe("left-hand side");
        Pipe rhsPipe = new Pipe("right-hand side");
        
        // Join the two pipes
        Pipe joinPipe = new HashJoin(   lhsPipe, new Fields("lhs_uid"),
                                        rhsPipe, new Fields("rhs_uid"),
                                        new LeftJoin());
        joinPipe = new Every(joinPipe, new Count());

        // Create some data
        InMemoryTap lhsTap = new InMemoryTap(new Fields("lhs_uid", "lhs_value"));
        TupleEntryCollector writer = lhsTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", 205));
        writer.add(new Tuple("2", 32));
        writer.close();
        
        InMemoryTap rhsTap = new InMemoryTap(new Fields("rhs_uid", "rhs_value"));
        writer = rhsTap.openForWrite(new LocalFlowProcess());
        writer.add(new Tuple("1", 44));
        writer.add(new Tuple("3", 98));
        writer.close();
        
        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(lhsPipe.getName(), lhsTap);
        sources.put(rhsPipe.getName(), rhsTap);
        
        new LocalFlowConnector().connect(sources, new NullSinkTap(), joinPipe).complete();
    }

    @Test
    public void testNoAggregationAfterHashJoinHadoop() throws Exception {
        // Create some data
        Hfs lhsTap = new Hfs(new SequenceFile(new Fields("lhs_uid", "lhs_value")), "build/test/testNoAggregationAfterHashJoinHadoop/lhs/");
        TupleEntryCollector writer = lhsTap.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple("1", 205));
        writer.add(new Tuple("2", 32));
        writer.add(new Tuple("1", 90));
        writer.add(new Tuple("3", 5));
        writer.close();
        
        Hfs rhsTap = new Hfs(new SequenceFile(new Fields("rhs_uid")), "build/test/testNoAggregationAfterHashJoinHadoop/rhs/");
        writer = rhsTap.openForWrite(new HadoopFlowProcess());
        writer.add(new Tuple("1"));
        writer.add(new Tuple("2"));
        writer.close();
        
        Pipe lhsPipe = new Pipe("left-hand side");
        Pipe rhsPipe = new Pipe("right-hand side");
        
        // Join the two pipes
        Pipe joinPipe = new HashJoin(   lhsPipe, new Fields("lhs_uid"),
                                        rhsPipe, new Fields("rhs_uid"),
                                        new InnerJoin());
        joinPipe = new Every(joinPipe, new Fields("lhs_value"), new Sum(new Fields("sum"), Integer.class));
        joinPipe = new Each(joinPipe, new Debug(true));

        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(lhsPipe.getName(), lhsTap);
        sources.put(rhsPipe.getName(), rhsTap);
        
        new HadoopFlowConnector().connect(sources, new NullSinkTap(), joinPipe).complete();
    }
}
