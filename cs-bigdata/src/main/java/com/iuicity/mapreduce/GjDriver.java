package com.iuicity.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               ws
 * Module ID:   00001
 * Comments:   高金数据需求
 * JDK version used:      JDK1.8
 * Namespace:           com.iuicity.mapreduce
 * @author：             p.z
 * Create Date：  2017-12-21
 * Modified By：   Administrator
 * Modified Date:  2017-12-21
 * Why & What is modified
 * Version:        V1.0
 */
public class GjDriver extends Configured implements Tool {
    @Override
    public int run(String[] strings) {
        Configuration conf = getConf();
        return 0;
    }
}