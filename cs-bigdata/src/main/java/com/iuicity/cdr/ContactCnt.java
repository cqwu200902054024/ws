package com.iuicity.cdr;

/**
 * Created by lucien on 12/23/15.
 */
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ContactCnt extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
        FileSystem fs = FileSystem.get(conf);
        String indir = args[0];
        String outdir = args[1];
        fs.delete(new Path(outdir), true);
        Job job = Job.getInstance(conf, "ContactCnt");
        job.setJarByClass(ContactCnt.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(ContactCntMap.class);
        job.setPartitionerClass(HashPartitioner.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, indir);
        FileOutputFormat.setOutputPath(job, new Path(outdir));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ContactCntMap extends Mapper<LongWritable, Text, Text, IntWritable> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            context.write(new Text(strs[0]), new IntWritable(strs[1].split("\\|").length));
        }
    }

    public static void main(String[] args) throws Exception {
        int errCode = ToolRunner.run(new Configuration(), new ContactCnt(), args);
        if (errCode == 0) {
            System.out.println("Success!");  
        } else {
            System.out.println("Error!");
        }
        System.exit(errCode);
    }
}