package com.iuicity.mapreduce.input;

import com.google.common.base.Charsets;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               ws
 * Module ID:   00001
 * JDK version used:      JDK1.8
 * Namespace:           com.iuicity.mapreduce.input
 * @author：             p.z
 * Create Date：  2017-12-21
 * Modified By：   Administrator
 * Modified Date:  2017-12-21
 * Why & What is modified
 * Version:        V1.0
 */
public class TextToMapInputFormat extends FileInputFormat<LongWritable,MapWritable>{
    @Override
    public RecordReader<LongWritable, MapWritable> createRecordReader(InputSplit split, TaskAttemptContext context) {
        String delimiter = context.getConfiguration().get(
                "textinputformat.record.delimiter");
        byte[] recordDelimiterBytes = null;
        if (null != delimiter) {
            recordDelimiterBytes = delimiter.getBytes(Charsets.UTF_8);
        }
        return new TextToMapRecordReader(recordDelimiterBytes);
    }
}
