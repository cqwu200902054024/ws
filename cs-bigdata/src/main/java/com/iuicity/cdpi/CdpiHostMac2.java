package com.iuicity.cdpi;

import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
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

public class CdpiHostMac2 extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",
				true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		conf.set("config", args[2]);
		fs.delete(new Path(outdir), true);
		Job job = Job.getInstance(conf, getClass().getSimpleName());
		job.setJarByClass(CdpiHostMac2.class);

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

	public static class CdpiHostPhoneMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		private Text ky = new Text();
		private Text va = new Text();  
		private  String pattern = ".+[0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F][:,-][0-9a-fA-F][0-9a-fA-F].*";
        // 创建 Pattern 对象
		private  Pattern r = Pattern.compile(pattern);
		private int refindex;
		private int cookieIndex;
		private int urlIndex;
		private String delimiter;
		private int hostIndex;
		
            
		@Override
		protected void setup(Context context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			FileSystem fs = FileSystem.get(conf);
			FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
			Properties prop = new Properties();
			prop.load(fsdis);
			this.refindex = Integer.valueOf(prop.getProperty("refindex"));
			this.cookieIndex = Integer.valueOf(prop.getProperty("cookieIndex"));
			this.urlIndex = Integer.valueOf(prop.getProperty("urlIndex"));
			this.hostIndex = Integer.valueOf(prop.getProperty("hostIndex"));
			this.delimiter = prop.getProperty("delimiterCdpi");
		}
		
		@Override  
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			    String[] sts =  value.toString().split(this.delimiter);
		        String url = URLDecoder.decode(sts[this.urlIndex],"utf-8");
				String ref = URLDecoder.decode(new String(Base64.decodeBase64(sts[this.refindex])),"utf-8");
				String cookie = sts[this.cookieIndex];
				Matcher urlm = r.matcher(url);
				Matcher refm = r.matcher(ref);
				Matcher cookiem = r.matcher(cookie);
			  if(urlm.matches() || refm.matches() || cookiem.matches()) {
				String host = sts[this.hostIndex];
				ky.set(host);
				va.set(url + "|" + ref + "|" + cookie);
		    	context.write(ky, va);
			  }
		};

		@SuppressWarnings("unused")
		private static String getHost(String url) {
			if (!(StringUtils.startsWithIgnoreCase(url, "http://") || StringUtils
					.startsWithIgnoreCase(url, "https://"))) {
				url = "http://" + url;
			}  
			String returnVal = StringUtils.EMPTY;
			try {
				URI uri = new URI(url);
				returnVal = uri.getHost();
			} catch (Exception e) {
			}
			
			if ((StringUtils.endsWithIgnoreCase(returnVal, ".html") || StringUtils
					.endsWithIgnoreCase(returnVal, ".htm"))) {
				returnVal = StringUtils.EMPTY;
			}
			
			return returnVal;
		} 
	}
    
	public static class CdpiHostPhoneReducer extends Reducer<Text, Text, Text, Text> {
		private TreeMap<Bean,List<String>> map;
		protected void setup(Context context) throws IOException ,InterruptedException {
	    	this.map = new TreeMap<Bean,List<String>>(new Comparator<Bean>() {
					@Override
					public int compare(Bean o1, Bean o2) {//
						return o2.getCount() - o1.getCount();
					};
				});
		};

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			List<String> list = new ArrayList<String>();
			int count = 0;
			for (Text value : values) {
				count ++;
				list.add(value.toString());
			}
			
			this.map.put(new Bean(key.toString(),count), list);
		};
		
		@Override
		protected void cleanup(Context context) throws IOException ,InterruptedException {
			for(Map.Entry<Bean,List<String>> kv : this.map.entrySet()) {
				for(String url : kv.getValue()) {
				context.write(new Text(kv.getKey().getName() + "\t" + kv.getKey().getCount()), new Text(url));
				}
			}
		};
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new CdpiHostMac2(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}
