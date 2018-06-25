package com.iuicity.bulkImport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class FindByMdTest {
	private static Configuration conf = null;
	private static HTable htable = null;
	static {
		conf = HBaseConfiguration.create();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "datanode3,datanode4,datanode5");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			htable = new HTable(conf, "idcard_md5");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) throws IOException {
		long start = System.currentTimeMillis();
		File jfile1 = new File(args[1]);
		if (!jfile1.exists()) {
			jfile1.createNewFile();
		}
		FileWriter fw1 = new FileWriter(jfile1.getAbsoluteFile());
		BufferedWriter bw1 = new BufferedWriter(fw1);
		File jfile2 = new File(args[2]);
		if (!jfile2.exists()) {
			jfile2.createNewFile();
		}
		FileWriter fw2 = new FileWriter(jfile2.getAbsoluteFile());
		BufferedWriter bw2 = new BufferedWriter(fw2);
		File file = new File(args[0]);
		
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Map<String,String> rowkeys = getMd5FromFile(reader);
		try {
				getResultByVersion(bw1,bw2,rowkeys);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			reader.close();
			bw1.close();
			bw2.close();
		}
		System.out.println("cost time:" + (System.currentTimeMillis() - start)/1000);
	}
	public static Map<String,String> getMd5FromFile(BufferedReader reader) throws IOException {
		Map<String,String> md5s = new LinkedHashMap<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			md5s.put(tempStr.split("\t")[1],tempStr.split("\t")[0]);
		}
		return md5s;
	}
    public static void getResultByVersion(BufferedWriter bw1,BufferedWriter bw2,Map<String,String> rowKeys) throws IOException {
    	for(Map.Entry<String, String> entry : rowKeys.entrySet()) {
            Get get = new Get(Bytes.toBytes(entry.getKey()));
            Result result = htable.get(get);
            if(result.isEmpty()) {
            	bw2.write(entry.getKey() + "\t" + entry.getValue());
            	bw2.write("\n");
            } else {
            	 String value =  new String(result.getValue(Bytes.toBytes("md5"), Bytes.toBytes("idcard")),"UTF-8");
                 bw1.write(entry.getKey() + "\t" + value);
                 bw1.write("\n");
            }
    	}
    }
}