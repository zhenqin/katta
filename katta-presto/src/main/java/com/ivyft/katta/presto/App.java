package com.ivyft.katta.presto;

import com.ivyft.katta.lib.lucene.ILuceneServer;

import java.lang.reflect.Field;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/20
 * Time: 18:28
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class App {

    public static void main(String[] args) throws Exception {
        Field versionID = ILuceneServer.class.getField("versionID");
        System.out.println(versionID.getLong(ILuceneServer.class));
    }
}
