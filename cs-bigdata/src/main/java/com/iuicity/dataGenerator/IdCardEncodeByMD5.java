package com.iuicity.dataGenerator;

import com.iuicity.util.EncoderHandler;
import com.iuicity.util.Utils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Set;


public class IdCardEncodeByMD5 {

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
	 * 枚举某段时间内所有日
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
	
	public static void main(String[] args) throws ParseException, IOException {
  	  String start = args[0];  
  	  String end = args[1];
  	  System.out.println(start + "-" + end );
  	  long start_time = System.currentTimeMillis();
  	  SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");  
  	  Date dBegin = sdf.parse(start);  
  	  Date dEnd = sdf.parse(end);  
  	  List<Date> lDate = listDates(dBegin,dEnd);  
  	  int areaCo = 0;
  	  String dateStr = "";
  	  String idCard =  "";
  	  FileOutputStream out = new FileOutputStream(new File(args[2]));  
      Set<Integer> codes = Utils.txtToSet("codes.txt");
      System.out.println(codes.size());
		for(Integer code : codes) {  
			areaCo = code; 
		    for(Date dt:lDate) {
	    	dateStr = sdf.format(dt);
		    	for(String rcode:listRandomCode()) {
		    		String idCard17 = areaCo + dateStr + rcode;
		    		idCard = areaCo + dateStr + rcode + calcTrailingNumber(idCard17.toCharArray());
		    		out.write((EncoderHandler.encodeByMD5(idCard) + "\t" + idCard).getBytes());
		    		out.write("\n".getBytes());
		    	}
		    }
		}
		out.close();
		System.out.println("generator over! take time: " + (System.currentTimeMillis() - start_time));
	}
}