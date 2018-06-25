package com.iuicity.cdr;

/**
 * Created by lucien on 15-12-4.
 */
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.lang.ArrayIndexOutOfBoundsException;

import org.apache.commons.codec.binary.Base64;
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

public class MdnUin extends Configured implements Tool {

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
		Job job = Job.getInstance(conf, "MdnUin");
		job.setJarByClass(MdnUin.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(MdnUinMap.class);
		job.setCombinerClass(MdnUinReduce.class);
		job.setReducerClass(MdnUinReduce.class);
		boolean isPartition = Boolean.valueOf(prop.getProperty("isPartition"));
		if (isPartition) {
			job.setPartitionerClass(HashPartitioner.class);
			job.setNumReduceTasks(Integer.valueOf(prop.getProperty("parNum")));
		}
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MdnUinMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		private int mdnIndex;
		private int hostIndex;
		private int refIndex;
		private boolean isRefBase64;
		private String delimiter;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			mdnIndex = Integer.valueOf(prop.getProperty("mdnIndex"));
			hostIndex = Integer.valueOf(prop.getProperty("hostIndex"));
			refIndex = Integer.valueOf(prop.getProperty("refIndex"));
			isRefBase64 = Boolean.valueOf(prop.getProperty("isRefBase64"));
			delimiter = prop.getProperty("delimiterCdpi");
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(delimiter);
			try {
				String mdn = strs[mdnIndex];
				String host = strs[hostIndex];
				if (mdn.length() > 0 && !mdn.equals("none")
						&& (host.contains("mmsns.qpic.cn") || host.contains("mmbiz.qpic.cn"))) {
					String ref;
					if (isRefBase64)
						ref = new String(Base64.decodeBase64(strs[refIndex]));
					else
						ref = strs[refIndex];
					if (ref.contains("uin=") && !ref.contains("uin=&")) {
						Pattern pattern = Pattern.compile("(?<=uin=).+?(?=&)|(?<=uin=).+");
						Matcher matcher = pattern.matcher(ref);
						if (matcher.find())
							context.write(new Text(mdn + "\t" + matcher.group()), new IntWritable(1));
					}
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class MdnUinReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
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
		int errCode = ToolRunner.run(new Configuration(), new MdnUin(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}