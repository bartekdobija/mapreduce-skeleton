package com.github.bartekdobija.reducers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OutputReducer extends Reducer<LongWritable, Text, Text, NullWritable> {

    private static final NullWritable NULL = NullWritable.get();

    @Override
    protected void reduce(final LongWritable key, final Iterable<Text> values,
            final Reducer<LongWritable, Text, Text, NullWritable>.Context context)
            throws IOException, InterruptedException {
        for(Text v : values){
            context.write(v, NULL);
            return;
        }
    }
}
