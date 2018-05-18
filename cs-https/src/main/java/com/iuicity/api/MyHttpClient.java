package com.iuicity.api;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 * @author wangwenli
 * 自定义httpclient工具类，跳过ssl验证访问rest请求
 */
public class MyHttpClient {
	private static Logger log=LoggerFactory.getLogger("CATALINA");
	
	/**
	 * @param urlstr 地址url
	 * @param map 参数map集合
	 * @param charset 编码
	 * @param authorization 头信息中放置的验证信息，tokenId
	 * @return
	 */
	public static String doPost(String urlstr,Map<String,String> map ,String charset,String authorization){
		   
		    HttpClient httpClient = null;
	        HttpPost httpPost = null;  
	        String responseStr = null; 
		try{  
            httpClient = (HttpClient) new SSLClient();  
            httpPost = new HttpPost(urlstr); 
            if(authorization!=null&&!"".equals(authorization)) {
				httpPost.addHeader("Authorization", authorization);
			}
            httpPost.addHeader("Content-Type", "application/x-www-form-urlencoded;charset="+charset);
            //设置参数
            if(map!=null&&map.size()>0){
            List<NameValuePair> list = new ArrayList<NameValuePair>();  
            Iterator iterator = map.entrySet().iterator();  
            while(iterator.hasNext()){  
                Map.Entry<String,String> elem = (Map.Entry<String, String>) iterator.next();  
                list.add(new BasicNameValuePair(elem.getKey(),elem.getValue()));  
            }  
            if(list.size() > 0){  
                UrlEncodedFormEntity entity = new UrlEncodedFormEntity(list,charset);  
                httpPost.setEntity(entity);  
            }  
            }
            HttpResponse response = httpClient.execute(httpPost);  
            if(response != null){  
                HttpEntity resEntity = response.getEntity();  
                if(resEntity != null){  
                	responseStr = EntityUtils.toString(resEntity,charset);  
                }  
            }  
        }catch(Exception ex){  
            ex.printStackTrace();  
        }  
		return responseStr;
	}
	/**
	 * @param urlstr 地址url
	 * @param map 参数map集合
	 * @param charset 编码
	 * @param authorization 头信息中放置的验证信息，tokenId
	 * @return
	 */
	public static String doGet(String urlstr,Map<String,String> map ,String charset,String authorization){
		log.info("before:"+urlstr);   
	    HttpClient httpClient = null;
        HttpGet httpGet = null;  
        String responseStr = null;
        StringBuffer sbf=new StringBuffer();
        if(map!=null&&map.size()>0){
        	Iterator<String> keys=map.keySet().iterator();
        	while(keys.hasNext()){
        		String key=keys.next();
        		String value=map.get(key);
        		sbf.append(key+"="+value+"&");
        	}
        }
        if(sbf.length()>0&&urlstr.indexOf("?")==-1){
        	urlstr+="?"+sbf.substring(0, sbf.length()-1);
        }else if(sbf.length()>0&&urlstr.indexOf("?")>-1){
        	urlstr+=sbf.substring(0, sbf.length()-1);
        }
        log.info("after:"+urlstr);   
        try {
        	httpGet=new HttpGet(urlstr);
			httpClient = (HttpClient) new SSLClient();
			if(authorization!=null&&!"".equals(authorization))
				httpGet.addHeader("Authorization", authorization);
			httpGet.addHeader("Content-Type", "application/x-www-form-urlencoded;charset="+charset);
			HttpResponse response = httpClient.execute(httpGet);  
            if(response != null){  
                HttpEntity resEntity = response.getEntity();  
                if(resEntity != null){  
                	responseStr = EntityUtils.toString(resEntity,charset);  
                }  
            }  
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return responseStr;
	}
	
	
	
	

}
