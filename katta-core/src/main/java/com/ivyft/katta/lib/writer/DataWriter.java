package com.ivyft.katta.lib.writer;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.MasterConfiguration;

import java.nio.ByteBuffer;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/29
 * Time: 11:37
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class DataWriter {


    public abstract void init(MasterConfiguration conf, InteractionProtocol protocol, String indexName);


    public abstract void write(String shardId, ByteBuffer objByte);


    public abstract void close();
}
