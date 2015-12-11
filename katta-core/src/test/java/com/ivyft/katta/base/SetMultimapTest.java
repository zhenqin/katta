package com.ivyft.katta.base;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.junit.Test;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/11
 * Time: 14:44
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class SetMultimapTest {


    /**
     *
     *
     */
    private SetMultimap<String, String> aliveKattaNodeMap = HashMultimap.create();


    @Test
    public void testPut() throws Exception {
        aliveKattaNodeMap.put("a", "aaa");
        aliveKattaNodeMap.put("a", "bbb");
        aliveKattaNodeMap.put("b", "aaa");


        aliveKattaNodeMap.put("a", "bbb");
        System.out.println(aliveKattaNodeMap);

    }
}
