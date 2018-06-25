package com.iuicity.crm;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

public class CRM extends Configured implements Tool {
	// public static int sum = 0;
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		conf.set("mdn", args[2]);
		conf.set("config", args[3]);
		fs.delete(new Path(outdir), true);
		Job job = Job.getInstance(conf, "CRM");
		job.setJarByClass(CRM.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(CRMMap.class);
		job.setCombinerClass(CRMReduce.class);
		job.setReducerClass(CRMReduce.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		boolean bResult = job.waitForCompletion(true);
		if (bResult) {
			return 0;
		}
		return 1;
	}

	public static HashMap<String, String> LoadData(String inData, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(inData), conf);
		FSDataInputStream fsdis = fs.open(new Path(inData));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis));

		FileSystem fs2 = FileSystem.get(conf);
		FSDataInputStream fsdis2 = fs2.open(new Path(conf.get("config")));
		Properties prop = new Properties();
		prop.load(fsdis2);

		HashMap<String, String> res = new HashMap<String, String>();
		String str;
		while ((str = br.readLine()) != null) {
			try {
				JSONObject jo = JSONObject.fromObject(str);
				res.put(jo.getString(prop.getProperty("sfz")), jo.getString(prop.getProperty("guid")));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		br.close();
		return res;
	}

	public static class CRMMap extends Mapper<LongWritable, Text, Text, Text> {
		private HashMap<String, String> map = new HashMap<String, String>();
		private int mdnIndex;
		private int flag;

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs2 = FileSystem.get(conf);
			FSDataInputStream fsdis2 = fs2.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis2);
			map = CRM.LoadData(conf.get("mdn"), conf);
			mdnIndex = Integer.valueOf(prop.getProperty("CRMmdnIndex"));
			flag = Integer.valueOf(prop.getProperty("remove_flagIndex"));
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] strs = value.toString().split("\t");
			String sfz = strs[strs.length - 1];
			String mdn = strs[mdnIndex];
			boolean normal = (strs[flag].equals("0"));
			if (!normal)
				return;
			if (map.containsKey(sfz)) {
				String newKey = map.get(sfz) + "\t" + mdn;
				context.write(new Text(newKey), new Text(" "));
			}
		}
	}

	public static class CRMReduce extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// String[] tokens = key.toString().split(":");
			try {
				context.write(key, new Text(" "));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new Configuration(), new CRM(), args);
		if (errCode == 0) {
			// System.out.println("people number is " + sum);
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}
