package com.iuicity.crm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

public class CountNum {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		int sumNum = 0;
		/// home/dmp_cs/wyh/crm/res1/
		/// home/dmp_cs/wyh/crm/res2/
		/// home/dmp_cs/wyh/crm/CRMres.txt
		String fhead1 = new String("/home/dmp_cs/wyh/crm/res1/");
		String fhead2 = new String("/home/dmp_cs/wyh/crm/res2/");
		File ftemp1 = new File(fhead1);
		String[] files1 = ftemp1.list();
		File ftemp2 = new File(fhead2);
		String[] files2 = ftemp2.list();
		ArrayList<String> fs = new ArrayList<String>();
		for (int i = 0; i < files1.length; i++) {
			fs.add(fhead1 + files1[i] + "/part-r-00000");
		}
		for (int i = 0; i < files2.length; i++) {
			fs.add(fhead2 + files2[i] + "/part-r-00000");
		}
		File fo = new File("/home/dmp_cs/wyh/crm/CRMres.txt");
		FileWriter fw = new FileWriter(fo);
		BufferedWriter bw = new BufferedWriter(fw);
		String str = new String();
		String newGuid = new String();
		int temp = 0;
		HashMap<String, Integer> map = new HashMap<String, Integer>();
		HashMap<String, HashSet<String>> mapSet = new HashMap<String, HashSet<String>>();
		for (String sf : fs) { 
			File f = new File(sf);
			FileReader fr = new FileReader(f);
			BufferedReader br = new BufferedReader(fr);
			while ((str = br.readLine()) != null) {
				temp = 1;
				String[] strs = str.split("\t");
				String guid = strs[0];
				String mdn = strs[1];
				if (!map.containsKey(guid)) {
					HashSet<String> set = new HashSet<String>();
					set.add(mdn);
					mapSet.put(guid, set);
					map.put(guid, 1);
					newGuid = guid + "101\t" + strs[1] + "\r\n";
					// System.out.println(newGuid);
					bw.write(newGuid);
					sumNum++;
				} else if ((temp = map.get(guid)) >= 5) {
					continue;
				} else {
					if (mapSet.get(guid).contains(mdn)) {
						continue;
					} else {
						mapSet.get(guid).add(mdn);
						temp++;
						map.put(guid, temp);
						newGuid = guid  + "10" + temp + "\t" + strs[1] + "\r\n";
						bw.write(newGuid);
						sumNum++;
					}
				}
			}
			br.close();
			fr.close();
		}
		bw.close();
		fw.close();
		System.out.println("number of people is " + map.size());
		System.out.println("number of mdn is " + sumNum);
	}
}
