package com.ivyft.katta.node;

import com.ivyft.katta.Katta;
import com.ivyft.katta.client.KattaAdmin;
import com.ivyft.katta.util.ZkConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/7
 * Time: 10:43
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaTest {


    KattaAdmin kattaAdmin;

    public KattaTest() {
    }


    @Before
    public void setUp() throws Exception {
        kattaAdmin = new KattaAdmin(new ZkConfiguration());
    }

    @Test
    public void addShard() throws Exception {
        kattaAdmin.addShard("userindex",
                "file:/Volumes/Study/IntelliJ/yiidata/katta1/data/lucene/P2D95Ggl2tWnSynu8Xg");
    }



    @Test
    public void removeShard() throws Exception {
        kattaAdmin.removeShard("userindex",
                 "userindex#P2D95Ggl2tWnSynu8Xg");
    }

    @Test
    public void testTimeUnit() throws Exception {
        System.out.println(System.currentTimeMillis());
        System.out.println(TimeUnit.MICROSECONDS.toMillis(1000));
        System.out.println(TimeUnit.MILLISECONDS.toMicros(1000));

    }

    @Test
    public void testCreateIndex() throws Exception {
        Katta.main(new String[]{"createIndex", "ts", "5", "10", "-s"});

    }
}
