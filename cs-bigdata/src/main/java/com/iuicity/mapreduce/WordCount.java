package com.iuicity.mapreduce;

import com.iuicity.mapreduce.input.TextToMapInputFormat;
import com.iuicity.mapreduce.mapper.WordCountMapper;
import com.iuicity.mapreduce.reducer.WordCountReducer;
import com.iuicity.util.MrUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.util.List;
import java.util.Map;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.wordcount
 * @author           p.z
 * Create Date  2017-12-08
 * Modified By   Administrator
 * Modified Date:  2017-12-08
 * Why & What is modified
 * Version:        V1.0
 */
public class WordCount extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        int resCode = ToolRunner.run(new WordCount(), args);

        if (resCode == 0) {
            System.out.println("SUCCESS!");
        } else {
            System.out.println("ERROR");
        }

        System.exit(resCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration configuration = getConf();
        for(Map.Entry<String,List<String>> entry : MrUtil.loadYamlToMap("mr.yaml").entrySet()) {
            configuration.setStrings(entry.getKey(),entry.getValue().toArray(new String[0]));
        }

        Job job = Job.getInstance(getConf());

        job.setJarByClass(WordCount.class);
        job.setJobName(this.getClass().getSimpleName());
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setInputFormatClass(TextToMapInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("D:\\data\\in\\in"));
        Path out =  new Path("D:\\data\\out\\test");
        FileSystem fs = FileSystem.get(configuration);
        fs.delete(out, true);
        FileOutputFormat.setOutputPath(job, out);

        return job.waitForCompletion(true)
               ? 0
               : 1;
    }//48
}
