package com.iuicity.cdr;

/**
 * Created by lucien on 15-12-4.
 */
import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
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

public class MdnMdn extends Configured implements Tool {

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
		Job job = Job.getInstance(conf, "MdnMdn");
		job.setJarByClass(MdnMdn.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setMapperClass(MdnMdnMap.class);
		job.setCombinerClass(MdnMdnCombine.class);
		job.setReducerClass(MdnMdnReduce.class);
		boolean isPartition = Boolean.valueOf(prop.getProperty("isPartition"));
		if (isPartition) {
			job.setPartitionerClass(HashPartitioner.class);
			job.setNumReduceTasks(Integer.valueOf(prop.getProperty("parNum")));
		}
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MdnMdnMap extends Mapper<LongWritable, Text, Text, IntWritable> {

		private Set<String> dianXin = new HashSet<>();
		private Set<String> yiDong = new HashSet<>();
		private Set<String> lianTong = new HashSet<>();
		private Set<String> others = new HashSet<>();
		private int callingIndex;
		private int calledIndex;
		private String delimiter;

		public void setup(Context context) throws IOException, InterruptedException {
			dianXin.add("133");
			dianXin.add("153");
			dianXin.add("180");
			dianXin.add("181");
			dianXin.add("189");
			dianXin.add("177");

			lianTong.add("130");
			lianTong.add("131");
			lianTong.add("132");
			lianTong.add("155");
			lianTong.add("156");
			lianTong.add("185");
			lianTong.add("186");
			lianTong.add("176");

			yiDong.add("134");
			yiDong.add("135");
			yiDong.add("136");
			yiDong.add("137");
			yiDong.add("138");
			yiDong.add("139");
			yiDong.add("150");
			yiDong.add("151");
			yiDong.add("152");
			yiDong.add("158");
			yiDong.add("159");
			yiDong.add("182");
			yiDong.add("183");
			yiDong.add("184");
			yiDong.add("157");
			yiDong.add("187");
			yiDong.add("188");
			yiDong.add("178");

			others.add("170");

			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			callingIndex = Integer.valueOf(prop.getProperty("callingIndex"));
			calledIndex = Integer.valueOf(prop.getProperty("calledIndex"));
			delimiter = prop.getProperty("delimiterCdr");
			Boolean.valueOf(prop.getProperty("zjTel"));
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split(delimiter);
			try {
				String calling = strs[callingIndex];
				String called = strs[calledIndex];
				if (calling.length() > 10 && called.length() > 10) {
					String mdn1 = calling.substring(calling.length() - 11, calling.length());
					String mdn2 = called.substring(called.length() - 11, called.length());
					if ((dianXin.contains(mdn1.substring(0, 3)) || yiDong.contains(mdn1.substring(0, 3))
							|| lianTong.contains(mdn1.substring(0, 3)) || others.contains(mdn1.substring(0, 3)))
							&& (dianXin.contains(mdn2.substring(0, 3)) || yiDong.contains(mdn2.substring(0, 3))
									|| lianTong.contains(mdn2.substring(0, 3))
									|| others.contains(mdn2.substring(0, 3)))) {
						context.write(new Text(mdn1 + '\t' + mdn2), new IntWritable(1));
						context.write(new Text(mdn2 + '\t' + mdn1), new IntWritable(1));
					}
				}
			} catch (ArrayIndexOutOfBoundsException e) {
				e.printStackTrace();
			}
		}
	}

	public static class MdnMdnCombine extends Reducer<Text, IntWritable, Text, IntWritable> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<IntWritable> it = values.iterator();
			int sum = 0;
			while (it.hasNext())
				sum += it.next().get();
			context.write(key, new IntWritable(sum));
		}
	}

	public static class MdnMdnReduce extends Reducer<Text, IntWritable, Text, Text> {
		protected void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			Iterator<IntWritable> it = values.iterator();
			int sum = 0;
			while (it.hasNext())
				sum += it.next().get();
			context.write(key, new Text(sum + "\t1"));
		}
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new Configuration(), new MdnMdn(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}