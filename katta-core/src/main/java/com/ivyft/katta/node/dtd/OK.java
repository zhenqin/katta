package com.ivyft.katta.node.dtd;

import java.io.Serializable;

/**
 *
 * 数据接收完成发送一个该标记。
 *
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/21
 * Time: 16:54
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class OK implements Serializable {


    /**
     * 序列化表格
     */
    private final static long serialVersionUID = 1L;


    protected final String message = "OK";


    public OK() {
    }

    public String getMessage() {
        return message;
    }
}
