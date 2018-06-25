package com.iuicity.cdr;

/**
 * Created by lucien on 15-12-4.
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Merge extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true);
        FileSystem fs=FileSystem.get(conf);
        String indir = args[0];
        String outdir = args[1];
        fs.delete(new Path(outdir),true);
        conf.set("config", args[2]);
        FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
        Properties prop = new Properties();
        prop.load(fsdis);
        Job job = Job.getInstance(conf, "Merge");
        job.setJarByClass(Merge.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(MergeMap.class);
        job.setCombinerClass(MergeReduce.class);
        job.setReducerClass(MergeReduce.class);
        boolean isPartition = Boolean.valueOf(prop.getProperty("isPartition"));
        if(isPartition) {
            job.setPartitionerClass(HashPartitioner.class);
            job.setNumReduceTasks(Integer.valueOf(prop.getProperty("parNum")));
        }
        FileInputFormat.setInputPaths(job, indir);
        FileOutputFormat.setOutputPath(job, new Path(outdir));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class MergeMap extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] strs = value.toString().split("\t");
                if (strs.length == 3)
                    context.write(new Text(strs[0] + "\t" + strs[1]), new Text(strs[2]));
                else
                    context.write(new Text(strs[0] + "\t" + strs[1]), new Text(strs[2]+"\t"+strs[3]));
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class MergeReduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            try {
                Iterator<Text> it = values.iterator();
                String[] value = it.next().toString().split("\t");
                if(value.length ==1) {
                    int sum = Integer.valueOf(value[0]);
                    while (it.hasNext())
                        sum += Integer.valueOf(it.next().toString());
                    context.write(key, new Text(String.valueOf(sum)));
                }
                else {
                    int sum = Integer.valueOf(value[0]);
                    int cntM = Integer.valueOf(value[1]);
                    while(it.hasNext()) {
                        value = it.next().toString().split("\t");
                        sum += Integer.valueOf(value[0]);
                        cntM += Integer.valueOf(value[1]);
                    }
                    context.write(key, new Text(String.valueOf(sum)+"\t"+String.valueOf(cntM)));
                }
            }
            catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int errCode = ToolRunner.run(new Configuration(), new Merge(), args);
        if (errCode == 0) {
            System.out.println("Success!");
        }
        else {
            System.out.println("Error!");
        }
        System.exit(errCode);
    }
}
