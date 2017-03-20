package com.ivyft.katta.client;

import com.ivyft.katta.protocol.CreateNewIndex;
import com.ivyft.katta.protocol.metadata.IndexDeployError;
import com.ivyft.katta.protocol.metadata.NewIndexMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
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
     * 新索引
     */
    protected final NewIndexMetaData newIndexMetaData;


    /**
     * 状态
     */
    protected IndexState indexState = IndexState.DEPLOYING;


    /**
     * 阻塞计数器
     */
    protected final CountDownLatch countDownLatch = new CountDownLatch(1);


    /**
     * 进程池
     */
    protected final Thread executor = new Thread(this);


    /**
     * log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(CreatedIndexDeployFuture.class);


    /**
     * 构造方法
     * @param meta
     */
    public CreatedIndexDeployFuture(NewIndexMetaData meta) {
        this.newIndexMetaData = meta;
        try {
            LOG.info("submit create index thread.");
            executor.start();
        } catch (Exception e){
            LOG.warn("create index error.", e);
        }
    }




    @Override
    public void run() {
        try {
            String dataStoragePath = newIndexMetaData.getPath();
            LOG.info("data storage path: " + dataStoragePath);

            CreateNewIndex createNewIndex = new CreateNewIndex(dataStoragePath, newIndexMetaData);
            createNewIndex.created();

            setIndexState(IndexState.DEPLOYED);
        } catch (IOException e) {
            LOG.warn("create index error.", e);
            setIndexState(IndexState.ERROR);
            IndexDeployError error = new IndexDeployError(newIndexMetaData.getName(), IndexDeployError.ErrorType.UNKNOWN);
            error.setException(e);

            newIndexMetaData.setDeployError(error);
        } finally {
            countDownLatch.countDown();
        }

    }


    @Override
    public synchronized IndexState joinDeployment() throws InterruptedException {
        return joinDeployment(Integer.MAX_VALUE);
    }


    @Override
    public synchronized IndexState joinDeployment(long maxWaitMillis) throws InterruptedException {
        try {
            countDownLatch.await(maxWaitMillis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            return getState();
        }
        return getState();
    }


    public void setIndexState(IndexState indexState) {
        this.indexState = indexState;
    }


    @Override
    public IndexState getState() {
        return indexState;
    }

}
