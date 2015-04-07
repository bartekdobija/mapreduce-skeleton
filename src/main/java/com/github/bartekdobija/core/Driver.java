package com.github.bartekdobija.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.github.bartekdobija.mappers.SampleMapper;
import com.github.bartekdobija.reducers.OutputReducer;

public class Driver extends Configured implements Tool {

    public static void showHelp() {
        System.out.println("usage: yarn jar mrstub.jar input output");
    }

    public static void main(final String[] args) throws Exception {
        if (args == null || args.length < 2) {
            showHelp();
            System.exit(1);
            return;
        }
        System.exit(ToolRunner.run(new Configuration(), new Driver(), args));
    }

    @Override
    public int run(final String[] args) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "Sample MapReduce");
        job.setJarByClass(Driver.class);
        job.setMapperClass(SampleMapper.class);
        job.setReducerClass(OutputReducer.class);
        //job.setOutputFormatClass();
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

}
