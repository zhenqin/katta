package com.ivyft.katta.operation.master;

import com.ivyft.katta.protocol.ConnectedComponent;
import com.ivyft.katta.protocol.IAddRemoveListener;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 16/4/15
 * Time: 12:01
 * Verdor: NowledgeData
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CommitIndexFuture implements IMasterFuture, IAddRemoveListener, ConnectedComponent {


    protected final String commitId;


    protected final InteractionProtocol protocol;


    protected final CountDownLatch countDownLatch = new CountDownLatch(1);


    protected State state = State.RUNNINGING;


    private static Logger LOG = LoggerFactory.getLogger(CommitIndexFuture.class);


    public CommitIndexFuture(String commitId, InteractionProtocol protocol) {
        this.commitId = commitId;
        this.protocol = protocol;
        protocol.registerChildListener(this, ZkConfiguration.PathDef.COMMIT, this);
    }


    private boolean isDeploymentRunning() {
        return getState() == State.RUNNINGING;
    }


    @Override
    public synchronized State joinDeployment() throws InterruptedException {
        countDownLatch.await();
        return getState();
    }

    @Override
    public synchronized State joinDeployment(long maxWaitMillis) throws InterruptedException {
        countDownLatch.await(maxWaitMillis, TimeUnit.MILLISECONDS);
        LOG.info("a commit success, commit id {}", this.commitId);
        return getState();
    }


    @Override
    public synchronized State getState() {
        return state;
    }

    @Override
    public void disposable() {
        protocol.unregisterChildListener(this, ZkConfiguration.PathDef.COMMIT);

        protocol.deleteZkPath(commitId);
    }

    @Override
    public void added(String name) {
        Map<String, Object> commitData = protocol.getCommitData(commitId);
        LOG.info(String.valueOf(commitData));

        state = State.STOPPED;

        if(StringUtils.equals(name, commitId)) {
            LOG.info("commit id {} success, the exec thread was notify.", name);
            countDownLatch.countDown();
        }
    }

    @Override
    public void removed(String name) {
        if(StringUtils.equals(name, commitId)) {
            disposable();
        }
    }

    @Override
    public void reconnect() {

    }

    @Override
    public void disconnect() {

    }
}
