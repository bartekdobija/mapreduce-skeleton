package com.github.bartekdobija.mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SampleMapper extends Mapper<LongWritable, Text, LongWritable, Text>{
    @Override
    protected void map(final LongWritable key, final Text value,
            final Mapper<LongWritable, Text, LongWritable, Text>.Context context)
            throws IOException, InterruptedException {
        context.write(key, value);
    }
}
