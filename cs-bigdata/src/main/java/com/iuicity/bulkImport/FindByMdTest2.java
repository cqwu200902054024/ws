package com.iuicity.bulkImport;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class FindByMdTest2 {
	private static Configuration conf = null;
	private static HTable htable = null;
	static {
		conf = HBaseConfiguration.create();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "datanode3,datanode4,datanode5");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		try {
			htable = new HTable(conf, "idcard");
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
		List<String> rowkeys = getMd5FromFile(reader);
		final int maxrowkeysize = Integer.valueOf(args[3]);
		int loopSize = rowkeys.size() % maxrowkeysize == 0 ? rowkeys.size() / maxrowkeysize
				: rowkeys.size() / maxrowkeysize + 1;
		try {
			for (int loop = 0; loop < loopSize; loop++) {
				int end = (loop + 1) * maxrowkeysize > rowkeys.size() ? rowkeys.size() : (loop + 1) * maxrowkeysize;
				List<String> partrowkeys = rowkeys.subList(loop * maxrowkeysize, end);
				getResultByVersion(bw1,bw2,partrowkeys);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			reader.close();
			bw1.close();
			bw2.close();
		}
		System.out.println("cost time:" + (System.currentTimeMillis() - start)/1000);
	}
	public static List<String> getMd5FromFile(BufferedReader reader) throws IOException {
		List<String> md5s = new ArrayList<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			md5s.add(tempStr.split("\t")[1]);
		}
		return md5s;
	}
    public static void getResultByVersion(BufferedWriter bw1,BufferedWriter bw2,List<String> rowKeys) throws IOException {
    	List<Get> gets = new ArrayList<Get>();
    	for (String md5 : rowKeys) {
			Get get = new Get(Bytes.toBytes(md5));
			get.addColumn("md5".getBytes(), "idcard".getBytes());
			gets.add(get);
		}
    	Result[] res = htable.get(gets);
    	for (Result re : res) {
    		if (re != null && !re.isEmpty()) {
                bw1.write(new String(re.getRow(),"UTF-8") + "\t" + new String(re.getValue("md5".getBytes(), "idcard".getBytes()),"UTF-8"));
                bw1.write("\n");
    		} else {
    			bw2.write(String.valueOf(re.getRow()));
    			bw2.write("\n");
    		}
    	}
    }
}