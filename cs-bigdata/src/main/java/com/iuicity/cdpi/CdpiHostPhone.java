package com.iuicity.cdpi;

import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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

public class CdpiHostPhone extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();
		conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",
				true);
		FileSystem fs = FileSystem.get(conf);
		String indir = args[0];
		String outdir = args[1];
		fs.delete(new Path(outdir), true);
		Job job = Job.getInstance(conf, getClass().getSimpleName());
		job.setJarByClass(CdpiHostPhone.class);

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
		private  String pattern = ".+\\D(1\\d{10})\\D.*";
        // 创建 Pattern 对象
		private  Pattern r = Pattern.compile(pattern);
           
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			   String[] sts =  value.toString().split("\\|");
				String url = sts[sts.length - 1];
				Matcher m = r.matcher(url);
			  if(m.matches()) {
				  System.out.println(url);
				ky.set(getHost(url));
				va.set(url);
		    	context.write(ky, va);
			  }
		};

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
		protected void setup(Context context) throws java.io.IOException ,InterruptedException {
	    	this.map = new TreeMap<Bean,List<String>>(new Comparator<Bean>() {
					@Override
					public int compare(Bean o1, Bean o2) {
						return o2.getCount() - o1.getCount();
					};
				});
		};

		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws java.io.IOException, InterruptedException {
			List<String> list = new ArrayList<String>();
			int count = 0;
			for (Text value : values) {
				count ++;
				list.add(value.toString());
			}
			this.map.put(new Bean(key.toString(),count), list);
		};
		
		@Override
		protected void cleanup(Context context) throws java.io.IOException ,InterruptedException {
			for(Map.Entry<Bean,List<String>> kv : this.map.entrySet()) {
				for(String url : kv.getValue()) {
				context.write(new Text(kv.getKey().getName() + "\t" + kv.getKey().getCount()), new Text(url));
				}
			}
		};
	}

	public static void main(String[] args) throws Exception {
		int errCode = ToolRunner.run(new CdpiHostPhone(), args);
		if (errCode == 0) {
			System.out.println("Success!");
		} else {
			System.out.println("Error!");
		}
		System.exit(errCode);
	}
}
