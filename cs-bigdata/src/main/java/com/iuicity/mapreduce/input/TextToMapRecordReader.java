package com.iuicity.mapreduce.input;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Seekable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.*;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CompressedSplitLineReader;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.SplitLineReader;

import java.io.IOException;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               ws
 * Module ID:   00001
 * Comments:  将每行数据转为Map
 * JDK version used:      JDK1.8
 * Namespace:           com.iuicity.mapreduce.input
 *
 * @author： p.z
 * Create Date：  2017-12-21
 * Modified By：   p.z
 * Modified Date:  2017-12-21
 * Why & What is modified
 * Version:        V1.0
 */
public class TextToMapRecordReader extends RecordReader<LongWritable,MapWritable>{
    //文件每行的最大长度
    public static final String MAX_LINE_LENGTH =
            "mapreduce.input.linerecordreader.line.maxlength";
    //每行的开始
    private long start;
    //读取每行时的偏移量
    private long pos;
    //每行的结束
    private long end;
    private SplitLineReader in;
    private FSDataInputStream fileIn;
    private Seekable filePosition;
    private int maxLineLength;
    private LongWritable key;
    private Text value;
    private MapWritable val;
    private byte[] recordDelimiterBytes;
    private String[] fields;

    public TextToMapRecordReader() {
    }

    public TextToMapRecordReader(byte[] recordDelimiter) {
        this.recordDelimiterBytes = recordDelimiter;
    }

    /**
     * 重写初始化函数
     * @param genericSplit
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit) genericSplit;
        Configuration job = context.getConfiguration();
        this.maxLineLength = job.getInt(MAX_LINE_LENGTH, Integer.MAX_VALUE);
        //初始化开始位置，获取分片的开始位置
        start = split.getStart();
        //结束位置，获取分片的结束位置
        end = start + split.getLength();
        final Path file = split.getPath();
        fields = job.getStrings(file.getName());

        // open the file and seek to the start of the split
        //打开文件并定位到分片的开始位置，从分片的开始位置读取
        final FileSystem fs = file.getFileSystem(job);
        fileIn = fs.open(file);

        fileIn.seek(start);
        in = new SplitLineReader(fileIn, job, this.recordDelimiterBytes);
        filePosition = fileIn;

        // If this is not the first split, we always throw away first record
        // because we always (except the last split) read one extra line in
        // next() method.
        if (start != 0) {
            start += in.readLine(new Text(), 0, maxBytesToConsume(start));
        }
        this.pos = start;
    }


    /**
     * 最大读取（消耗）字节计数器
     * @param pos  偏移量
     * @return
     */
    private int maxBytesToConsume(long pos) {//
        return  (int) Math.max(Math.min(Integer.MAX_VALUE, end - pos), maxLineLength);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (key == null) {
            key = new LongWritable();
        }
        key.set(pos);
        if (value == null) {
            value = new Text();
            val = new MapWritable();
        }
        int newSize = 0;
        // We always read one extra line, which lies outside the upper
        // split limit i.e. (end - 1)
        while (getFilePosition() <= end || in.needAdditionalRecordAfterSplit()) {
            if (pos == 0) {
                newSize = skipUtfByteOrderMark();
            } else {
                newSize = in.readLine(value, maxLineLength, maxBytesToConsume(pos));
                pos += newSize;
            }
            if ((newSize == 0) || (newSize < maxLineLength)) {
                break;
            }

        }
        if (newSize == 0) {
            key = null;
            value = null;
            return false;
        } else {
            return true;
        }
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public MapWritable getCurrentValue() throws IOException, InterruptedException {
        val.clear();
        String[] data = value.toString().split("\t");
        for(int i =0;i < fields.length; i++) {
            val.put(new Text(fields[i]),new Text(data[i]));
        }
        return val;
    }

    private long getFilePosition() throws IOException {
        return pos;
    }



    private int skipUtfByteOrderMark() throws IOException {
        // Strip BOM(Byte Order Mark)
        // Text only support UTF-8, we only need to check UTF-8 BOM
        // (0xEF,0xBB,0xBF) at the start of the text stream.
        int newMaxLineLength = (int) Math.min(3L + (long) maxLineLength,
                Integer.MAX_VALUE);
        int newSize = in.readLine(value, newMaxLineLength, maxBytesToConsume(pos));
        System.out.println("value===" + value.toString());
        // Even we read 3 extra bytes for the first line,
        // we won't alter existing behavior (no backwards incompat issue).
        // Because the newSize is less than maxLineLength and
        // the number of bytes copied to Text is always no more than newSize.
        // If the return size from readLine is not less than maxLineLength,
        // we will discard the current line and read the next line.
        pos += newSize;
        int textLength = value.getLength();
        byte[] textBytes = value.getBytes();
        if ((textLength >= 3) && (textBytes[0] == (byte)0xEF) &&
                (textBytes[1] == (byte)0xBB) && (textBytes[2] == (byte)0xBF)) {
            // find UTF-8 BOM, strip it.
            textLength -= 3;
            newSize -= 3;
            if (textLength > 0) {
                // It may work to use the same buffer and not do the copyBytes
                textBytes = value.copyBytes();
                value.set(textBytes, 3, textLength);
            } else {
                value.clear();
            }
        }
        return newSize;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (start == end) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (getFilePosition() - start) / (float)(end - start));
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (in != null) {
                in.close();
            }
        } finally {
        }
    }
}