package com.iuicity.cdr;

/**
 * Created by lucien on 12/8/15.
 */
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Map.Entry;

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
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Inverse extends Configured implements Tool {

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
        Job job = Job.getInstance(conf, "Inverse");
        job.setJarByClass(Inverse.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(InverseMap.class);
        job.setReducerClass(InverseReduce.class);
        boolean isPartition = Boolean.valueOf(prop.getProperty("isPartition"));
        if (isPartition) {
            job.setPartitionerClass(HashPartitioner.class);
            job.setNumReduceTasks(Integer.valueOf(prop.getProperty("parNum")));
        }
        FileInputFormat.setInputPaths(job, indir);
        FileOutputFormat.setOutputPath(job, new Path(outdir));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class InverseMap extends Mapper<LongWritable, Text, Text, Text> {
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String[] tops = strs[1].split("\\|");
            for (int i = 0; i != tops.length; ++i)
                context.write(new Text(tops[i].split(":")[0]), new Text(strs[0] + ":" + tops[i].split(":")[1]));
        }
    }

    public static class InverseReduce extends Reducer<Text, Text, Text, Text> {

        private boolean isValuesLimit;
        private int valuesLimitNum;

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            FileSystem fs = FileSystem.get(conf);
            FSDataInputStream fsdis = fs.open(new Path(conf.get("config")));
            Properties prop = new Properties();
            prop.load(fsdis);
            isValuesLimit = Boolean.valueOf(prop.getProperty("isValuesLimit"));
            valuesLimitNum = Integer.valueOf(prop.getProperty("valuesLimitNum"));
        }

        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Map<String, Integer> hm = new HashMap<>();
            Iterator<Text> it = values.iterator();
            String[] tmp;
            int cnt = 0;
            while (it.hasNext()) {
                ++cnt;
                if (isValuesLimit && cnt > valuesLimitNum)
                    break;
                tmp = it.next().toString().split(":");
                if (!tmp[1].isEmpty() && StringUtils.isNumeric(tmp[1]))
                    hm.put(tmp[0], Integer.valueOf(tmp[1]));
            }
            if (isValuesLimit) {
                if (cnt <= valuesLimitNum) {
                    List<Entry<String, Integer>> hmSorted = new ArrayList<>(hm.entrySet());
                    Collections.sort(hmSorted, new Comparator<Entry<String, Integer>>() {
                        @Override
                        public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                            // TODO Auto-generated method stub
                            return o2.getValue() - o1.getValue();
                        }
                    });
                    String tw = "";
                    for (int i = 0; i != hmSorted.size(); ++i) {
                        tw += hmSorted.get(i).getKey() + ":" + hmSorted.get(i).getValue() + "|";
                    }
                    if (!tw.isEmpty()) {
                        context.write(key, new Text(tw));
                    }
                }
            } else {
                List<Entry<String, Integer>> hmSorted = new ArrayList<>(hm.entrySet());
                Collections.sort(hmSorted, new Comparator<Entry<String, Integer>>() {
                    @Override
                    public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
                        // TODO Auto-generated method stub
                        return o2.getValue() - o1.getValue();
                    }
                });
                String tw = "";
                for (int i = 0; i != hmSorted.size(); ++i) {
                    tw += hmSorted.get(i).getKey() + ":" + hmSorted.get(i).getValue() + "|";
                }
                if (!tw.isEmpty()) {
                    context.write(key, new Text(tw));
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        int errCode = ToolRunner.run(new Configuration(), new Inverse(), args);
        if (errCode == 0) {
            System.out.println("Success!");
        } else {  
            System.out.println("Error!");
        }
        System.exit(errCode);
    }
}