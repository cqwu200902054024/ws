package com.iuicity.bulkImport;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
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
	
	public static String tolowercase(String md5) {
		return md5.toLowerCase();
	}
}