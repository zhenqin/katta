package com.ivyft.katta.client;

import com.ivyft.katta.protocol.CreateNewIndex;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.NewIndexMetaData;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/7
 * Time: 09:10
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CreatedIndexDeployFuture implements IIndexDeployFuture, Runnable {


    /**
     * 接口协议
     */
    protected InteractionProtocol protocol;


    /**
     * 新索引
     */
    protected NewIndexMetaData newIndexMetaData;


    /**
     * 状态
     */
    protected IndexState indexState = IndexState.DEPLOYING;


    /**
     * 阻塞计数器
     */
    protected CountDownLatch countDownLatch = new CountDownLatch(1);


    /**
     * 当前是否正在运行
     */
    protected boolean operating = false;


    /**
     * 进程池
     */
    protected ExecutorService executor = Executors.newSingleThreadExecutor();


    /**
     * 构造方法
     * @param protocol
     * @param meta
     */
    public CreatedIndexDeployFuture(InteractionProtocol protocol, NewIndexMetaData meta) {
        this.protocol = protocol;
        this.newIndexMetaData = meta;
    }




    @Override
    public void run() {
        try {
            String dataStoragePath = newIndexMetaData.getPath();

            CreateNewIndex createNewIndex = new CreateNewIndex(dataStoragePath, newIndexMetaData);
            createNewIndex.created();

            //尝试创建索引
            protocol.createIndex(newIndexMetaData);

            setIndexState(IndexState.DEPLOYED);
        } catch (IOException e) {
            setIndexState(IndexState.ERROR);
            IndexDeployError error = new IndexDeployError(newIndexMetaData.getName(), IndexDeployError.ErrorType.UNKNOWN);
            error.setException(e);

            newIndexMetaData.setDeployError(error);
        } finally {
            try {
                executor.shutdown();
            } catch (Exception e) {

            }
            countDownLatch.countDown();
        }

    }


    @Override
    public IndexState joinDeployment() throws InterruptedException {
        return joinDeployment(Integer.MAX_VALUE);
    }

    @Override
    public IndexState joinDeployment(long maxWaitMillis) throws InterruptedException {
        if(!operating) {
            operating = true;
            try {
                executor.submit(this);
            } catch (Exception e){

            }
        }

        try {
            countDownLatch.await(maxWaitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {

        }
        return getState();
    }


    public synchronized void setIndexState(IndexState indexState) {
        this.indexState = indexState;
    }


    @Override
    public IndexState getState() {
        return indexState;
    }

}
