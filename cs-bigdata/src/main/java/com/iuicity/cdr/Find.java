package com.iuicity.cdr;

/**
 * Created by lucien on 15-12-15.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import net.sf.json.JSONException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.sf.json.JSONObject;

public class Find extends Configured implements Tool {

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
		conf.set("findName", prop.getProperty("findName"));
		Job job = Job.getInstance(conf, "Find");
		job.setJarByClass(Find.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(FindMap.class);
		if (Boolean.valueOf(prop.getProperty("isFindCdrHistory")))
			job.setNumReduceTasks(0);
		else {
			job.setReducerClass(FindReduce.class);
			job.setNumReduceTasks(1);
		}
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class FindMap extends Mapper<LongWritable, Text, Text, Text> {

		private Set<String> input = new HashSet<>();
		private boolean isFindCdrHistory;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			input = ReadFileLineHashSet(prop.getProperty("input"), conf);
			isFindCdrHistory = Boolean.valueOf(prop.getProperty("isFindCdrHistory"));
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			if (strs.length > 1 && input.contains(strs[0])) {
				if (isFindCdrHistory)
					context.write(new Text(strs[0]), new Text(strs[1]));
				else {
					InputSplit inputSplit = context.getInputSplit();
					String dirName = ((FileSplit) inputSplit).getPath().getParent().getName();
					context.write(new Text(strs[0]), new Text(strs[1] + "\t" + dirName));
				}
			}
		}
	}

	public static class FindReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			String[] strs = it.next().toString().split("\t");
			String tw = strs[0];
			int min = Integer.valueOf(strs[1]);
			while (it.hasNext()) {
				strs = it.next().toString().split("\t");
				int num = Integer.valueOf(strs[1]);
				if (num < min) {
					min = num;
					tw = strs[0];
				}
			}
			context.write(key, new Text(tw));
		}
	}

	public static Set<String> ReadFileLineHashSet(String HDFSIndir, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(HDFSIndir), conf);
		FSDataInputStream fsdis = fs.open(new Path(HDFSIndir));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
		String tmpString;
		Set<String> res = new HashSet<>();
		while ((tmpString = br.readLine()) != null) {
			JSONObject ja;
			try {
				ja = JSONObject.fromObject(tmpString);
				res.add(ja.getString(conf.get("findName")));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		br.close();
		return res;
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new Configuration(), new Find(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);  
	}
}