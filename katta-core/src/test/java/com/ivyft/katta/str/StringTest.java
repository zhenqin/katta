package com.ivyft.katta.str;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.collections.ArrayStack;
import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.*;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 16:13
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class StringTest {

    public StringTest() {
    }


    @Test
    public void testCompare() throws Exception {
        List<String> srcs = new ArrayList<String>();
        for (int i = 0; i < 10; i++) {
            srcs.add(DigestUtils.md5Hex(String.valueOf(new Random().nextInt(1000000000))));
        }

        List<String> list = Arrays.asList(",7", "7,9", "9,a6", "a6,a9", "a9,f0", "f0,");

        for (String str : srcs) {
            for (String s : list) {
                String[] splits = s.split(",", -1);
                System.out.println(str + "  inner: [" + splits[0] + "->" + splits[1] + "] = " +
                        new Pair(splits[0], splits[1]).inner(str));
            }

            System.out.println("====================================");
        }

    }
}

class Pair implements Comparable<String> {

    protected final String start;


    protected final String end;

    public Pair(String start, String end) {
        this.start = start;
        this.end = end;
    }



    public int inner(String str) {
        boolean inner = (StringUtils.isBlank(start) || str.compareTo(start) > 0) && (StringUtils.isBlank(end) || str.compareTo(end) < 0);
        return inner ? 0 : -1;
    }



    public String getStart() {
        return start;
    }

    public String getEnd() {
        return end;
    }

    @Override
    public int compareTo(String s) {
        return 0;
    }
}
