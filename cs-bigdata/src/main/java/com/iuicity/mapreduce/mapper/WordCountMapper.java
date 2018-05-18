package com.iuicity.mapreduce.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               hadoop
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.wordcount
 * Author��             Administrator
 * Create Date��  2017-12-08
 * Modified By��   Administrator
 * Modified Date:  2017-12-08
 * Why & What is modified
 * Version:        V1.0
 */
public class WordCountMapper extends Mapper<LongWritable, MapWritable, Text, IntWritable> {
    private static final IntWritable ONE    = new IntWritable(1);
    private Text                     outKey = new Text();

    @Override
    protected void map(LongWritable key, MapWritable value, Context context) throws IOException, InterruptedException {
        context.write((Text)value.get(new Text("name")),ONE);
    }
}