package com.iuicity.bulkImport;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class AddIdCardGenerator {
	public static final Set<Integer> areaCode = new HashSet<>();

	private static Configuration conf = null;

	static {
		areaCode.add(120102);
		conf = HBaseConfiguration.create();
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "datanode3,datanode4,datanode5");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
	}
    
	public static char calcTrailingNumber(char[] chars) {
		if (chars.length < 17) { 
			return ' ';
		}
		int[] c = { 7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2 };
		char[] r = { '1', '0', 'X', '9', '8', '7', '6', '5', '4', '3', '2' };
		int[] n = new int[17];
		int result = 0;
		for (int i = 0; i < n.length; i++) {
			n[i] = Integer.parseInt(chars[i] + "");
		}
		for (int i = 0; i < n.length; i++) {
			result += c[i] * n[i];
		}
		return r[result % 11];
	}
   
	/**
	 * 枚举某段时间内所有日期
	 * @param dBegin
	 * @param dEnd
	 * @return
	 */
	public static List<Date> listDates(Date dBegin, Date dEnd) {
		List<Date> lDate = new ArrayList<Date>();
		lDate.add(dBegin);
		Calendar calBegin = Calendar.getInstance();
		calBegin.setTime(dBegin);
		Calendar calEnd = Calendar.getInstance();
		calEnd.setTime(dEnd);
		while (dEnd.after(calBegin.getTime())) {
			calBegin.add(Calendar.DAY_OF_MONTH, 1);
			lDate.add(calBegin.getTime());
		}
		return lDate;
	}
    
	public static List<String> listRandomCode() {
		List<String> codeList = new ArrayList<String>();
		for(int i = 0; i < 1000; i++) {
			if(i < 10) {
				codeList.add("00" + i);
			}
			
		   if(10 <= i && i < 100 ) {
				codeList.add("0" + i);
			}
			
		  if(i >= 100) {
				codeList.add(i + "");
			}
		}
		
		return codeList;
	}
	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		System.out.println("start============");
  	  String start = args[0];  
  	  String end = args[1];  
  	  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");  
  	  Date dBegin = sdf.parse(start);  
  	  Date dEnd = sdf.parse(end);  
  	  List<Date> lDate = listDates(dBegin,dEnd); 
  	  HTable table = new HTable(conf, args[2]);
  	  table.setAutoFlush(false);
  	  table.setWriteBufferSize(128 * 1024);
  	  int areaCo = 0;
  	  String dateStr = "";
  	  String idCard =  "";
		for(Integer code:AddIdCardGenerator.areaCode) {
		 	 areaCo = code;  
		    for(Date dt:lDate) {
		    	dateStr = sdf.format(dt);
		    	for(String rcode:listRandomCode()) {
		    		String idCard17 = areaCo + dateStr + rcode;
		    		idCard = areaCo + dateStr + rcode + calcTrailingNumber(idCard17.toCharArray());
		    		Put put = new Put(Bytes.toBytes(EncoderHandler.encodeByMD5(idCard)));// 指定行
		    		put.add(Bytes.toBytes("md5"), Bytes.toBytes("idcard"),
		    				Bytes.toBytes(idCard));
		    		table.put(put);
		    	}
		    }
		}  
		table.close();
	    //System.out.println("game over=======================");
	}  
}