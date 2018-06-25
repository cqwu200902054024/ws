package com.iuicity.bulkImport;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public class HBaseTest {
	public static class HBaseTestMapper extends  TableMapper<Text, Text> {
		private List<String> rowkeys = new ArrayList<>(1000000);
		private Text rk = new Text();
		private Text dt = new Text();
		@SuppressWarnings("deprecation")
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			BufferedReader in = null;
			Path[] paths = context.getLocalCacheFiles();
			String rowkey = null;
			for (Path path : paths) {
				if (path.getName().contains("test")) {
					in = new BufferedReader(new FileReader(path.getName()));
					while (null != (rowkey = in.readLine())) {
						rowkeys.add(rowkey.split("\t")[0]);
					}
				}
			}
		}
		    
		 @Override
		protected void map(ImmutableBytesWritable key, Result value,Context context)
				throws IOException, InterruptedException {
			 //String ip = Bytes.toString(row.get()).split("-")[0];  
			 String rowkey = Bytes.toString(key.get());
			 String date = Bytes.toString(value.getValue(Bytes.toBytes("md5"), Bytes.toBytes("idcard")));
			 if(rowkeys.contains(rowkey)) {
				 rk.set(rowkey);
				 dt.set(date);
				context.write(rk, dt); 
			 }
		}
	}
	
	public static class HBaseTestReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for(Text value : values) {
				context.write(key, value);
			}
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		 Configuration conf = HBaseConfiguration.create();  
         Job job = Job.getInstance(conf, "test");
         job.setJarByClass(HBaseTest.class);     

         Scan scan = new Scan();
         scan.setCaching(500);        
         scan.setCacheBlocks(false);  // don't set to true for MR jobs  
         // set other scan attrs  
         //scan.addColumn(family, qualifier);  
         TableMapReduceUtil.initTableMapperJob(  
                 "idcard",        // input table  
                 scan,               // Scan instance to control CF and attribute selection  
                 HBaseTestMapper.class,     // mapper class  
                 Text.class,         // mapper output key  
                 IntWritable.class,  // mapper output value  
                 job);  
         
         job.setNumReduceTasks(1);   // at least one, adjust as required  
         boolean b = job.waitForCompletion(true);  
         if (!b) {  
             throw new IOException("error with job!");  
         }   
	}
}
