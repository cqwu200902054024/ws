package com.iuicity.cdpi;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

public class CdpiHostMac3 extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		conf.set("config", args[2]);
		fs.delete(new Path(outdir), true);
		Job job = Job.getInstance(conf, getClass().getSimpleName());
		job.setJarByClass(CdpiHostMac3.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(CdpiHostPhoneMapper.class);
		job.setReducerClass(CdpiHostPhoneReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class CdpiHostPhoneMapper extends Mapper<LongWritable, Text, Text, Text> {

		private Text ky = new Text();
		private Text va = new Text();
		private String pattern = ".+([0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F]).*";
		// 创建 Pattern 对象
		private Pattern r = Pattern.compile(pattern);
		private int urlIndex;
		private int phoneIndex;
		private String delimiter;
		private List<String> macs = new ArrayList<String>();

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			this.urlIndex = Integer.valueOf(prop.getProperty("urlIndex"));
			this.phoneIndex = Integer.valueOf(prop.getProperty("phoneIndex"));
			this.delimiter = prop.getProperty("delimiterCdpi");

			BufferedReader in = null;
			Path[] paths = context.getLocalCacheFiles();
			String host = null;
			for (Path path : paths) {
				if (path.getName().contains("macs")) {
					in = new BufferedReader(new FileReader(path.getName()));
					while (null != (host = in.readLine())) {
						macs.add(host);
					}
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] sts = value.toString().split(this.delimiter);
			String url = URLDecoder.decode(sts[this.urlIndex], "utf-8");
			Matcher urlm = r.matcher(url);
			if (urlm.matches()) {
				String phone = sts[this.phoneIndex];
				String mac = urlm.group(1);
				if (this.macs.contains(mac)) {
					ky.set(phone);
					va.set(mac);
					context.write(ky, va);
				}
			}
		};
	}

	public static class CdpiHostPhoneReducer extends Reducer<Text, Text, Text, Text> {

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				context.write(key, value);
			}
		};
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new CdpiHostMac3(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}
