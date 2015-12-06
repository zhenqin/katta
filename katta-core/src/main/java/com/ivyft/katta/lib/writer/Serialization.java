package com.ivyft.katta.lib.writer;

import com.ivyft.katta.codec.Serializer;

import java.io.IOException;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/28
 * Time: 12:22
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * com.ivyft.katta.lib.writer.Serialization
 *
 * @author zhenqin
 */
public interface Serialization {

    public abstract byte getContentTypeId();

    public abstract String getContentType();

    public abstract Serializer serialize()
            throws IOException;
}
