package com.ivyft.katta.protocol;

import org.I0Itec.zkclient.IZkDataListener;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午10:06
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ZkDataListenerAdapter extends ListenerAdapter implements IZkDataListener {


    /**
     * ZooKeeper上的数据变动Listener
     */
    private final IZkDataListener dataListener;


    /**
     *
     * @param dataListener Listener
     * @param path 变动的节点
     */
    public ZkDataListenerAdapter(IZkDataListener dataListener, String path) {
        super(path);
        this.dataListener = dataListener;
    }


    @Override
    public void handleDataChange(String dataPath, Object data) throws Exception {
        this.dataListener.handleDataChange(dataPath, data);
    }

    @Override
    public void handleDataDeleted(String dataPath) throws Exception {
        this.dataListener.handleDataDeleted(dataPath);
    }

}
