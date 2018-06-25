package com.iuicity.cdr;

/**
 * Created by lucien on 12/23/15.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;
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

public class ProcessCdrTop extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive", true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		conf.set("mccs", args[2]);
		conf.set("mcs", args[3]);
		conf.set("data", args[4]);
		conf.set("config", args[5]);
		fs.delete(new Path(outdir), true);
		Job job = Job.getInstance(conf, "ProcessCdrTop");
		job.setJarByClass(ProcessCdrTop.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setMapperClass(ProcessCdrTopMap.class);
		job.setReducerClass(ProcessCdrTopReduce.class);
		job.setNumReduceTasks(1);
		FileInputFormat.setInputPaths(job, indir);
		FileOutputFormat.setOutputPath(job, new Path(outdir));
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class ProcessCdrTopMap extends Mapper<LongWritable, Text, Text, Text> {

		private ArrayList<MdnCContacts> mccs = new ArrayList<>();
		private ArrayList<Double> data = new ArrayList<>();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			mccs = ReadFileLineArrayList(conf.get("mccs"), conf.get("mcs"), conf);
			data = LoadData(conf.get("data"), conf);
		}

		protected Double calculate(String contacts, String _contacts) {
			double score = 0;
			String[] strs = contacts.split("\\|");
			HashMap<String, Integer> contactCnt = new HashMap<>();
			for (int i = 0; i != strs.length; ++i) {
				String[] tmp = strs[i].split(":");
				contactCnt.put(tmp[0], Integer.valueOf(tmp[1]));//二度联系人以及联系个数
			}
			strs = _contacts.split("\\|");//最近1月通话记录
			String contact;
			for (int i = 0; i != strs.length; ++i) {
				contact = strs[i].split(":")[0];//
				if (contactCnt.containsKey(contact) && contactCnt.get(contact) < data.size())
					score += 1 / data.get(contactCnt.get(contact));//权重分之一
			}
			return score;
		}

		protected Integer count(String contacts, String _contacts) {
			int cnt = 0;
			String[] _contactsArr = _contacts.split("\\|");
			for (int i = 0; i != _contactsArr.length; ++i) {
				if (contacts.contains(_contactsArr[i].split(":")[0]))
					++cnt;
			}
			return cnt;
		}

		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] strs = value.toString().split("\t");
			for (int i = 0; i != mccs.size(); ++i) {
				if (mccs.get(i).ccontacts.contains(strs[0]) && !mccs.get(i).mdn.equals(strs[0])) {
					//二度联系人中可能有一个是自己，从最近一个月中筛选出是自己
					double score = calculate(mccs.get(i).contacts, strs[1]);//求出相似度得分
					int cnt = count(mccs.get(i).contacts, strs[1]);//求出重复个数，权重
					if (cnt > 2)
						context.write(new Text(mccs.get(i).mdn), new Text(strs[0] + ":" + cnt + "\t" + score));
				}
			}
		}
	}

	public static class ProcessCdrTopReduce extends Reducer<Text, Text, Text, NullWritable> {

		private int topNum;
		private Map<String, String> idMdn = new HashMap<>();

		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			topNum = Integer.valueOf(prop.getProperty("topNum"));
			idMdn = ReadFileLineHashMap(prop.getProperty("input"), conf);
		}

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			//context.write(new Text(idMdn.get(key.toString())), new Text(FindTop(values, topNum)));
			context.write(new Text(FindTop(values, topNum,idMdn.get(key.toString()))),NullWritable.get());
		}
	}

	public static String FindTop(Iterable<Text> vals, int top) {//找出分数最高的钱topN个
		Iterator<Text> it = vals.iterator();
		double[] max = new double[top];
		String[] strTop = new String[top];
		String[] tmp;
		String res = "";
		double count;
		int size = 0;
		while (it.hasNext()) {
			tmp = it.next().toString().split("\t");
			count = Double.valueOf(tmp[1]);//相似度分数
			int i = top - 1;
			while (i >= 0 && max[i] < count) {
				if (i != top - 1) {
					max[i + 1] = max[i];
					if (strTop[i] != null)
						strTop[i + 1] = strTop[i];
				}
				
				--i;
			}
			
			if (i != top - 1) {
				max[i + 1] = count;
				strTop[i + 1] = tmp[0];
			}
			++size;
		}
		
		if (size >= top) {
			for (int i = 0; i != top; ++i)
				res += strTop[i] + ":" + String.format("%.2f", max[i]) + "|";//手机号码:重复个数:得分|
		} else {
			for (int i = 0; i != size; ++i)
				res += strTop[i] + ":" + String.format("%.2f", max[i]) + "|";
		}
		return res;
	}

	public static String FindTop(Iterable<Text> vals, int top,String guid) {//找出分数最高的钱topN个
		Iterator<Text> it = vals.iterator();
		double[] max = new double[top];
		String[] strTop = new String[top];
		String[] tmp;
		String res = guid + '3';
		double count;
		int size = 0;
		while (it.hasNext()) {
			tmp = it.next().toString().split("\t");
			count = Double.valueOf(tmp[1]);//相似度分数
			int i = top - 1;
			while (i >= 0 && max[i] < count) {
				if (i != top - 1) {
					max[i + 1] = max[i];
					if (strTop[i] != null)
						strTop[i + 1] = strTop[i];
				}
				--i;
			}
			
			if (i != top - 1) {
				max[i + 1] = count;
				strTop[i + 1] = tmp[0];
			}
			++size;
		}
		
		if (size >= top) {//20161019010001100 
			for (int i = 0; i != top; ++i) {
				//res += strTop[i] + ":" + String.format("%.2f", max[i]) + "|";//手机号码:重复个数:得分|
				int rep = Integer.valueOf(strTop[i].split(":")[1]);
				String iphone = strTop[i].split(":")[0];
				res += rep + i + "\t" + iphone; 
			}
		} else {
			for (int i = 0; i != size; ++i) {
				//res += strTop[i] + ":" + String.format("%.2f", max[i]) + "|";
			int rep = Integer.valueOf(strTop[i].split(":")[1]);
			String iphone = strTop[i].split(":")[0];
			 res += rep + i + "\t" + iphone; 
			}
		}
		return res;
	}
	
	public static ArrayList<MdnCContacts> ReadFileLineArrayList(String mccsHDFSIndir, String mcsHDFSIndir,
			Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(mccsHDFSIndir), conf);
		FSDataInputStream fsdis = fs.open(new Path(mccsHDFSIndir));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
		String tmpString;
		String[] strs;
		ArrayList<MdnCContacts> res = new ArrayList<>();
		while ((tmpString = br.readLine()) != null) {
			MdnCContacts mcc = new MdnCContacts();
			mcc.mdn = tmpString.split("\t")[0];
			strs = tmpString.split("\t")[1].split("\\|");
			for (int i = 0; i != strs.length; ++i)
				mcc.ccontacts.add(strs[i].split(":")[0]);// 二度联系人
			res.add(mcc);
		}
		br.close();
		fs = FileSystem.get(URI.create(mcsHDFSIndir), conf);
		fsdis = fs.open(new Path(mcsHDFSIndir));
		br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
		while ((tmpString = br.readLine()) != null) {
			strs = tmpString.split("\t");
			for (int i = 0; i != res.size(); ++i) {
				if (res.get(i).mdn.equals(strs[0])) {
					res.get(i).contacts = strs[1];
					break;
				}
			}
		}
		br.close();
		return res;
	}

	public static ArrayList<Double> LoadData(String inData, Configuration conf) throws IOException {
		FileSystem fs = FileSystem.get(URI.create(inData), conf);
		FSDataInputStream fsdis = fs.open(new Path(inData));
		BufferedReader br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
		String tmp;
		ArrayList<Double> res = new ArrayList<>();
		while ((tmp = br.readLine()) != null)
			res.add(Double.valueOf(tmp));
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
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		br.close();
		return res;
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new Configuration(), new ProcessCdrTop(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}

class MdnCContacts {
	String mdn;// 手机号
	String contacts = "";// 通话人数
	Set<String> ccontacts = new HashSet<>();// 二度联系人
}