package com.iuicity.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
public class Utils { 
	public static Set<Integer> txtToSet(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<Integer> idcard6 = new HashSet<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			idcard6.add(Integer.valueOf(tempStr.trim()));
		}
		reader.close();
		return idcard6;
	}
	
	public static Set<Integer> txtToSet2(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<Integer> idcard6 = new HashSet<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			idcard6.add(Integer.valueOf(tempStr.split("\t")[1].substring(0, 6)));
		}
		reader.close();
		return idcard6;
	}
	
	public static Set<String> txtToSet3(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<String> phonecodes = new HashSet<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			phonecodes.add(tempStr.split("\t")[1]);
		}
		reader.close();
		return phonecodes;
	}
	
	public static Set<String> txtToSet4(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<String> phonecodes = new HashSet<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			phonecodes.add(tempStr.split("\t")[0]);
		}
		reader.close();
		return phonecodes;
	}
	
	public static Set<String> txtToSet5(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<String> phonecodes = new HashSet<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			phonecodes.add(tempStr.trim());
		}
		reader.close();
		return phonecodes;
	}
	
	public static Map<String,String> txtToMap(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Map<String,String> phonecodes = new HashMap<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			phonecodes.put(tempStr.split("\t")[1],tempStr.split("\t")[0]);
		}
		reader.close();
		return phonecodes;
	}
	
	public static Set<String> txtToSet6(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<String> phonecodes = new HashSet<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			//System.out.println(tempStr.split(",")[1]);
			phonecodes.add(tempStr.split(",")[1].replace("\"",""));
		}
		reader.close();
		return phonecodes;
	}
	
	public static Set<Integer> txtToSet7(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		Set<Integer> phonecodes = new HashSet<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			if(tempStr.length() > 6) {
			phonecodes.add(Integer.valueOf(tempStr.trim().substring(0, 6)));
			}
		}
		reader.close();
		return phonecodes;
	}
	
	public static List<String> txtToSet8(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> datas = new ArrayList<>();
		String tempStr = null;
		while ((tempStr = reader.readLine()) != null) {
			datas.add(tempStr.trim().split(",")[1]);
		}
		reader.close();
		return datas;
	}
	
	public static List<String> txtToSet9(String path) throws IOException {
		File file = new File(path);
		BufferedReader reader = new BufferedReader(new FileReader(file));
		List<String> datas = new ArrayList<>();
		String tempStr = null;
		
		while ((tempStr = reader.readLine()) != null) {
			datas.add(tempStr.trim());
		}
		reader.close();
		return datas;
	}

	public static String encodeBySM3(String str) throws IOException{
		return SM3.byteArrayToHexString(SM3.hash(str.getBytes()));
	}
}