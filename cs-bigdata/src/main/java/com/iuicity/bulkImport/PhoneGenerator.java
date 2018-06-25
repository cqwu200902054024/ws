package com.iuicity.bulkImport;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;


public class PhoneGenerator {
	public static List<String> listRandomCode2() {
		List<String> codeList = new ArrayList<String>();
		for (int i = 0; i < 10000; i++) {
			if (i < 10) {
				codeList.add("000" + i);
			}

			if (10 <= i && i < 100) {
				codeList.add("00" + i);
			}

			if (i >= 100 && i < 1000) {
				codeList.add("0" + i);
			}
			
			if (i >= 1000) {
				codeList.add(i + "");
			}
		}
		return codeList;
	}
	
	
	public static void main(String[] args) throws ParseException, IOException {
	 long start_time = System.currentTimeMillis();
  	  FileOutputStream out = new FileOutputStream(new File(args[0]));  
      Set<String> codes = Utils.txtToSet3("phonecodes.txt");
      System.out.println(codes.size());
		for(String code : codes) {
		    	for(String rcode:listRandomCode2()) {
		    		String phone = code + rcode;
		    		out.write((EncoderHandler.encodeByMD5(phone) + "\t" + phone).getBytes());
		    		out.write("\n".getBytes());
		    	}
		}
		out.close();
		System.out.println("generator over! take time: " + (System.currentTimeMillis() - start_time));
	}
}