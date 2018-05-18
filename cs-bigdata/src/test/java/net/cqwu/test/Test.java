package net.cqwu.test;

import com.iuicity.util.MrUtil;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               ws
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu.test
 *
 * @author： Administrator
 * Create Date：  2017-12-25
 * Modified By：   Administrator
 * Modified Date:  2017-12-25
 * Why & What is modified
 * Version:        V1.0
 */
public class Test {

    @org.junit.Test
    public void testYaml() {
        StringBuffer sb = new StringBuffer();
        sb.append("123456789\t");
        sb.delete(sb.length() - 1,sb.length());
        System.out.println(sb.toString() + "===");
    }
}
