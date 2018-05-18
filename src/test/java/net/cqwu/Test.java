package net.cqwu;

/**
 * CopyRright (c)2018-0000:   net.cqwu
 * Project:               ws
 * Module ID:   00001
 * Comments:
 * JDK version used:      JDK1.8
 * Namespace:           net.cqwu
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
    public void testS() {
        String str = "nihao,fdf , ,";
        System.out.println(str.trim().split("\\s*,\\s*")[2]);
    }
}
