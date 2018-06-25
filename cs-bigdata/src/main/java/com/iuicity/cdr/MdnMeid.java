package com.iuicity.cdr;

/**
 * Created by lucien on 15-12-4.
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.lang.ArrayIndexOutOfBoundsException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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

public class MdnMeid extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		fs.delete(new Path(outdir), true);
		conf.set("config", args[2]);
		FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
		Properties prop = new Properties();
		prop.load(fsdis);
		Job job = Job.getInstance(conf, "MdnMeid");
		job.setJarByClass(MdnMeid.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(MdnMeidMap.class);
		job.setCombinerClass(MdnMeidReduce.class);
		job.setReducerClass(MdnMeidReduce.class);
		boolean isPartition = Boolean.valueOf(prop.getProperty("isPartition"));
		if (isPartition) {
			job.setPartitionerClass(HashPartitioner.class);
			job.setNumReduceTasks(Integer.valueOf(prop.getProperty("parNum")));
		}
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MdnMeidMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		private int mdnIndex;
		private int meidIndex;
		private String delimiter;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			mdnIndex = Integer.valueOf(prop.getProperty("mdnIndex"));
			meidIndex = Integer.valueOf(prop.getProperty("meidIndex"));
			delimiter = prop.getProperty("delimiterCdpi");
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(delimiter);
			try {
				String mdn = strs[mdnIndex];
				String meid = strs[meidIndex];
				if (!mdn.isEmpty() && !meid.isEmpty()) {
					context.write(new Text(mdn + "\t" + meid), new IntWritable(1));
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}

		}
	}

	public static class MdnMeidReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<IntWritable> it = values.iterator();
			int sum = 0;
			while (it.hasNext())
				sum += it.next().get();
			context.write(key, new IntWritable(sum));
		}
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new Configuration(), new MdnMeid(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}