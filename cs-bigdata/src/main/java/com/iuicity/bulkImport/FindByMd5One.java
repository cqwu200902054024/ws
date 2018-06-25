package com.iuicity.bulkImport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class FindByMd5One {
	private static Configuration conf = null;
	private static HTable htable = null;
	static {
		conf = HBaseConfiguration.create();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "datanode3,datanode4,datanode5");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		htable = new HTable(conf, args[0]);
		File jfile1 = new File(args[4]);
		if (!jfile1.exists()) {
			jfile1.createNewFile();
		}
		FileWriter fw = new FileWriter(jfile1.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		File file = new File(args[3]);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Map<String,String> md5s = getMd5FromFile(reader);
		List<String> rowkeys = new ArrayList<>(md5s.keySet());
		try {
				getResultByVersion(bw,rowkeys,args[1],args[2],md5s);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			reader.close();
			bw.close();
			bw.close();
		}
		System.out.println("cost time:" + (System.currentTimeMillis() - start)/1000);
	}
	
	public static Map<String,String> getMd5FromFile(BufferedReader reader) throws IOException {
		Map<String,String> md5s = new HashMap<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			md5s.put(tempStr.trim().toLowerCase(),tempStr.trim());
		}
		return md5s;
	}
	
    public static void getResultByVersion(BufferedWriter bw,List<String> rowKeys,String rowkey,String qualifier,Map<String,String> md5s) throws IOException {
    	for(String rowKey : rowKeys) {
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = htable.get(get);
            if(result.isEmpty()) {
            	bw.write(md5s.get(rowKey) + "\t" + "");
            	bw.write("\n");
            } else {
            	 String value =  new String(result.getValue(Bytes.toBytes(rowkey), Bytes.toBytes(qualifier)),"UTF-8");
            	 bw.write(md5s.get(rowKey) + "\t" + value);
            	 bw.write("\n");
            }
    	}
    }
}