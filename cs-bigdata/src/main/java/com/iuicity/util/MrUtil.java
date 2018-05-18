package com.iuicity.util;

import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.List;
import java.util.Map;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               ws
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           com.iuicity.util
 *
 * @author： Administrator
 * Create Date：  2017-12-25
 * Modified By：   Administrator
 * Modified Date:  2017-12-25
 * Why & What is modified
 * Version:        V1.0
 */
public class MrUtil {
    private static  Yaml yaml;
    static {
        yaml = new Yaml();
    }
    public static Map<String,List<String>> loadYamlToMap(String fileName) {
        return  yaml.load(MrUtil.class.getClassLoader().getResourceAsStream(fileName));
    }
}