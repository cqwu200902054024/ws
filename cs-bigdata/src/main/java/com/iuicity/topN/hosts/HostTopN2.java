package com.iuicity.topN.hosts;

import java.io.IOException;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HostTopN2 extends Configured implements Tool {

	public static class HostMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		private int hostIndex;
		private String delimiter;
		private Text hosts = new Text();
		private LongWritable ONE = new LongWritable(1);

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			hostIndex = Integer.valueOf(prop.getProperty("hostIndex"));
			delimiter = prop.getProperty("delimiterCdpi");
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(this.delimiter);
			String host = strs[this.hostIndex];
			hosts.set(host);
			context.write(hosts, ONE);
		}
	}

	public static class HostReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		private LongWritable val = new LongWritable();

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long count = 0l;
			for (LongWritable value : values) {
				count += value.get();
			}

			val.set(count);
			context.write(key, val);
		}

	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		fs.delete(new Path(outdir), true);
		conf.set("config", args[2]);
		Job job = Job.getInstance(conf, getClass().getSimpleName());
		job.setJarByClass(HostTopN2.class);
		
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		
		job.setMapperClass(HostMapper.class);
		job.setCombinerClass(HostReducer.class);
		job.setReducerClass(HostReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new HostTopN2(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}