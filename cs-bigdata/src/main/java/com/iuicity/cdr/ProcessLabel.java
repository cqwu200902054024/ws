package com.iuicity.cdr;

/**
 * Created by lucien on 12/23/15.
 */
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;
import java.util.Map.Entry;

public class ProcessLabel extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		conf.set("input", args[2]);
		conf.set("config", args[3]);
		fs.delete(new Path(outdir), true);
		Job job = Job.getInstance(conf, "ProcessLabel");
		job.setJarByClass(ProcessLabel.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(ProcessLabelMap.class);
		job.setReducerClass(ProcessLabelReduce.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ProcessLabelMap extends Mapper<LongWritable, Text, Text, Text> {

		ArrayList<KV> kvs = new ArrayList<>();

		protected void setup(Context context) throws IOException, InterruptedException {
			kvs = ReadFileLine(context.getConfiguration().get("input"), context.getConfiguration());
		}

		// 查找二度联系人
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");//输入格式  uin	mdn:1|mdn:2|
			String[] tops = strs[1].split("\\|");//
			for (int i = 0; i != kvs.size(); ++i) {
				for (int j = 0; j != kvs.get(i).values.size(); ++j) {
					if (kvs.get(i).values.get(j).equals(strs[0])) {//strs[0] 微信号
						//tops[0]手机号  tops[1]登录次数
						if (tops[0].split(":")[0].equals(kvs.get(i).key) && tops.length > 1) {
							//手机号 
							context.write(new Text(kvs.get(i).key), new Text((j + 1) + ":" + tops[1].split(":")[0]));
						} else if (!tops[0].split(":")[0].equals(kvs.get(i).key)) {
							context.write(new Text(kvs.get(i).key), new Text((j + 1) + ":" + tops[0].split(":")[0]));
						}
					}
				}
			}
		}
	}

	public static class ProcessLabelReduce extends Reducer<Text, Text, Text, NullWritable> {

		private Map<String, String> idMdn = new HashMap<>();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			idMdn = ReadFileLineHashMap(prop.getProperty("input"), conf);
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			Iterator<Text> it = values.iterator();
			Map<String, Integer> hm = new HashMap<>();
			String[] tmp;
			while (it.hasNext()) {
				tmp = it.next().toString().split(":");
				if (StringUtils.isNumeric(tmp[0]))
					hm.put(tmp[1], Integer.valueOf(tmp[0]));//手机号	个数
			}
			
			List<Entry<String, Integer>> hmSorted = new ArrayList<>(hm.entrySet());
			Collections.sort(hmSorted, new Comparator<Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					return o2.getValue() - o1.getValue();
				}
			});
			String tw = "";
			for (int i = 0; i != hmSorted.size(); ++i) {
				//tw += hmSorted.get(i).getValue() + ":" + hmSorted.get(i).getKey() + "|";
				tw += i + "\t" + hmSorted.get(i).getKey();
			}
			if (!tw.isEmpty()) {
				context.write(new Text(idMdn.get(key.toString()) + "20" + tw),NullWritable.get());
			}
		}
	}

	public static ArrayList<KV> ReadFileLine(String HDFSIndir, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(HDFSIndir), conf);
		FSDataInputStream fsdis = fs.open(new Path(HDFSIndir));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
		String tmpString;
		String[] strs1;
		String[] strs2;
		ArrayList<KV> res = new ArrayList<>();
		while ((tmpString = br.readLine()) != null) {
			strs1 = tmpString.split("\t");
			KV mu = new KV();
			mu.key = strs1[0];//手机号    
			strs2 = strs1[1].split("\\|");
			for (int i = 0; i != strs2.length; ++i)
				mu.values.add(strs2[i].split(":")[0]);// 微信号
			res.add(mu);
		}
		br.close();
		return res;
	}

	public static Map<String, String> ReadFileLineHashMap(String HDFSIndir, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(HDFSIndir), conf);
		FSDataInputStream fsdis = fs.open(new Path(HDFSIndir));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
		String tmpString;
		Map<String, String> res = new HashMap<>();
		while ((tmpString = br.readLine()) != null) {
			JSONObject ja;
			try {
				ja = JSONObject.fromObject(tmpString);
				res.put(ja.getString(conf.get("tel")), ja.getString(conf.get("caseid")));
			} catch (JSONException e) {
				e.printStackTrace();
			}
		}
		br.close();
		return res;
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new Configuration(), new ProcessLabel(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}

class KV {
	String key;
	ArrayList<String> values = new ArrayList<>();
}