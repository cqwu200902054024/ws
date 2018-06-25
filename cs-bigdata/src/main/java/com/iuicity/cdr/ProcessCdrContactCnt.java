package com.iuicity.cdr;

/**
 * Created by lucien on 12/23/15.
 */
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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

public class ProcessCdrContactCnt extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        Configuration conf = getConf();
        conf.setBoolean("mapreduce.input.fileinputformat.input.dir.recursive",true);
        FileSystem fs=FileSystem.get(conf);
        String indir = args[0];
        String outdir = args[1];
        conf.set("mcs", args[2]);
        fs.delete(new Path(outdir),true);
        Job job = Job.getInstance(conf, "ProcessCdrContactCnt");
        job.setJarByClass(ProcessCdrContactCnt.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(ProcessCdrContactCntMap.class);
        job.setReducerClass(ProcessCdrContactCntReduce.class);
        job.setNumReduceTasks(1);
        FileInputFormat.setInputPaths(job, indir);
        FileOutputFormat.setOutputPath(job, new Path(outdir));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class ProcessCdrContactCntMap extends Mapper<LongWritable, Text, Text, Text> {

        ArrayList<MdnContacts> mcs = new ArrayList<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            mcs = ReadFileLineArrayList(context.getConfiguration().get("mcs"), context.getConfiguration());
        }

        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strs = value.toString().split("\t");
            String mdn = strs[0];
            String cnt = strs[1];
            for(int i = 0; i != mcs.size(); ++i) {
                if(mcs.get(i).contacts.contains(mdn))
                    context.write(new Text(mcs.get(i).mdn), new Text(mdn+":"+cnt));
            }
        }
    }

    public static class ProcessCdrContactCntReduce extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String tw = "";
            Iterator<Text> it = values.iterator();
            while(it.hasNext())
                tw += it.next().toString()+"|";
            context.write(key, new Text(tw));
        }
    }


    public static ArrayList<MdnContacts> ReadFileLineArrayList(String mcsHDFSIndir, Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(URI.create(mcsHDFSIndir), conf);
        FSDataInputStream fsdis = fs.open(new Path(mcsHDFSIndir));
        BufferedReader br = new BufferedReader(new InputStreamReader(fsdis, "UTF-8"));
        String tmpString;
        String[] strs;
        ArrayList<MdnContacts> res = new ArrayList<>();
        while((tmpString = br.readLine()) != null) {
            MdnContacts mcc = new MdnContacts();
            mcc.mdn = tmpString.split("\t")[0];
            strs = tmpString.split("\t")[1].split("\\|");
            for(int i = 0; i != strs.length; ++i)
                mcc.contacts.add(strs[i].split(":")[0]);
            res.add(mcc);
        }
        br.close();
        return res;
    }

    public static void main(String[] args) throws Exception {
        int errCode = ToolRunner.run(new Configuration(), new ProcessCdrContactCnt(), args);
        if (errCode == 0) {
            System.out.println("Success!");
        }
        else {
            System.out.println("Error!");
        }
        System.exit(errCode);
    }
}

class MdnContacts {
    String mdn;
    Set<String> contacts = new HashSet<>();
}