package com.github.bartekdobija;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import com.github.bartekdobija.mappers.SampleMapper;
import com.github.bartekdobija.reducers.OutputReducer;

public class SampleMapperReducerTest {

    MapDriver<LongWritable, Text, LongWritable, Text> mapDriver;
    ReduceDriver<LongWritable, Text, Text, NullWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, LongWritable, Text, Text, NullWritable> mapReduceDriver;
    LongWritable key = new LongWritable(1);
    Text data = new Text("data");

    @Before
    public void setUp() {
      SampleMapper mapper = new SampleMapper();
      OutputReducer reducer = new OutputReducer();
      mapDriver = MapDriver.newMapDriver(mapper);
      reduceDriver = ReduceDriver.newReduceDriver(reducer);
      mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() throws IOException {
        mapDriver.withInput(key ,data);
        mapDriver.withOutput(key, data);
        mapDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        List<Text> list = new ArrayList<Text>();
        list.add(data);
        reduceDriver.withInput(key, list);
        reduceDriver.withOutput(data, NullWritable.get());
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        mapReduceDriver.withInput(key, data);
        mapReduceDriver.withOutput(data , NullWritable.get());
        mapReduceDriver.runTest();
    }

}
