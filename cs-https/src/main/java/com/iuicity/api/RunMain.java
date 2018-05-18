package com.iuicity.api;

import java.util.HashMap;
import java.util.Map;

public class RunMain {

	public static void main(String[] args) {
		//参数信息
		Map<String,String> map =new HashMap<String,String>();
		map.put("name", "");
		map.put("certNo", "");
		map.put("bankCard", "");
		map.put("serialNo", "");
		//分配的tokenId
		String tokenId="Bearer 4b0ff830baa62cc3a20d3551e239e9a6";
		//访问地址
		String url="https://api.data-u.com:8243/201/credit_bank_three";
		String response=MyHttpClient.doPost(url, map, "UTF-8",tokenId);
		//System.out.println(response);
	}
}