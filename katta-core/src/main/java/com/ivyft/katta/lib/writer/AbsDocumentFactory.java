package com.ivyft.katta.lib.writer;

import java.nio.ByteBuffer;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 14:11
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class AbsDocumentFactory<T> implements DocumentFactory<T> {


    @Override
    public List<T> deserial(SerdeContext context, ByteBuffer buffer) {
        return null;
    }
}
