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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

@SuppressWarnings("deprecation")
public class FindByMd5MultiThread {
	
	private static Configuration conf = null;
	private static  HTable  htable = null;
	
	static {
		conf = HBaseConfiguration.create();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "datanode3,datanode4,datanode5");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}

	public static void main(String[] args) throws IOException {
		File jfile = new File(args[4]);
		htable = new HTable(conf, args[0]);
		if (!jfile.exists()) {
			jfile.createNewFile();
		}

		FileWriter fw = new FileWriter(jfile.getAbsoluteFile());
		BufferedWriter bw = new BufferedWriter(fw);
		
		File file = new File(args[3]);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Map<String,String> md5s = getMd5FromFile(reader);
		List<String> rowkeys = new ArrayList<>(md5s.keySet());
		long start = System.currentTimeMillis();
		try {
			Map<String, String> datas = getDataFromHbase(rowkeys,Integer.valueOf(args[5]),args[1],args[2]);
			for(Map.Entry<String, String> data : datas.entrySet()) {
				bw.write(md5s.get(data.getKey()) + "\t" + data.getValue());
				bw.write("\n");
			}
		} catch (InterruptedException | ExecutionException e) {
			e.printStackTrace();
		} finally {
			reader.close();
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

	public static ConcurrentHashMap<String, String> getDataFromHbase(final List<String> rowkeys,int batchSize,String rowkey,String qualifier)
			throws InterruptedException, ExecutionException {
		if (rowkeys == null || rowkeys.isEmpty()) {
			return new ConcurrentHashMap<String, String>();
		}

		final int maxrowkeysize = batchSize;
		ExecutorService pool = Executors.newFixedThreadPool(5);
		int loopSize = rowkeys.size() % maxrowkeysize == 0 ? rowkeys.size() / maxrowkeysize
				: rowkeys.size() / maxrowkeysize + 1;
		ConcurrentHashMap<String, String> md5idcards = new ConcurrentHashMap<>();
		List<Future<ConcurrentHashMap<String, String>>> futures = new ArrayList<>();
		for (int loop = 0; loop < loopSize; loop++) {
			int end = (loop + 1) * maxrowkeysize > rowkeys.size() ? rowkeys.size() : (loop + 1) * maxrowkeysize;
			List<String> partrowkeys = rowkeys.subList(loop * maxrowkeysize, end);
			Future<ConcurrentHashMap<String, String>> future = pool.submit(new BathMd5Callable(partrowkeys,htable,rowkey,qualifier));
			futures.add(future);
		}
		pool.shutdown();

		for (Future<ConcurrentHashMap<String, String>> future : futures) {
			md5idcards.putAll(future.get());
		}

		return md5idcards;
	}

}

class BathMd5Callable implements Callable<ConcurrentHashMap<String, String>> {
	private List<String> md5s;
	private HTable htable = null;
	private String rowkey;
	private String qualifier;
	public BathMd5Callable(List<String> keys,HTable htable,String rowkey,String qualifier) {
		this.md5s = keys;
		this.htable = htable;
		this.rowkey = rowkey;
		this.qualifier = qualifier;
	}

	@Override
	public ConcurrentHashMap<String, String> call() throws Exception {
		return getIdcardByMd5s(md5s);
	}

	protected ConcurrentHashMap<String, String> getIdcardByMd5s(List<String> md5s) throws IOException {
		ConcurrentHashMap<String, String> rets = null;
		List<Get> gets = new ArrayList<Get>();
		for (String md5 : md5s) {
			Get get = new Get(Bytes.toBytes(md5));
			get.addColumn(rowkey.getBytes(), qualifier.getBytes());
			gets.add(get);
		}

		Result[] res = htable.get(gets);
		if (res != null && res.length > 0) {
			rets = new ConcurrentHashMap<String, String>(res.length);
			for (Result re : res) {
				if (re != null && !re.isEmpty()) {
					rets.put(new String(re.getRow(),"UTF-8"),new String(re.getValue(rowkey.getBytes(), qualifier.getBytes()),"UTF-8"));
				}
			}
		}
		return rets;
	}
}