package com.ivyft.katta.operation.node;

import com.ivyft.katta.node.NodeContext;
import com.ivyft.katta.protocol.NodeQueue;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 *
 * 处理Master分发给该节点的事件
 *
 * </p>
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午8:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class NodeOperationProcessor  implements Runnable {


    /**
     * Master 分发给Node的事件入这个队列
     */
    private final NodeQueue queue;


    /**
     * Node Context
     */
    private final NodeContext nodeContext;



    protected final String nodeName;


    /**
     * log
     */
    protected final static Logger LOG = LoggerFactory.getLogger(NodeOperationProcessor.class);


    /**
     * 构造方法要传入重要的实例
     * @param queue
     * @param nodeContext
     */
    public NodeOperationProcessor(NodeQueue queue, NodeContext nodeContext) {
        this.queue = queue;
        this.nodeContext = nodeContext;
        this.nodeName = this.nodeContext.getNode().getName();
    }

    @Override
    public void run() {
        try {
            while (nodeContext.getNode().isRunning()) {
                try {
                    //TODO 一直在ZooKeeper上读取数据，如果取不到，会进入死循环，直到取到数据。
                    NodeOperation operation = this.queue.peek();
                    if(operation != null) {
                        OperationResult operationResult;
                        try {
                            LOG.info("start executing " + operation);
                            long time = System.currentTimeMillis();
                            //执行
                            operationResult = operation.execute(this.nodeContext);
                            LOG.info("executed " + operation + " use time: " + (System.currentTimeMillis() - time));
                        } catch (Exception e) {
                            LOG.error(nodeName + ": failed to execute " + operation, e);
                            operationResult = new OperationResult(nodeName, e);
                        }
                        //如果执行异常，会传入null
                        this.queue.complete(operationResult);// only remove after finish
                    }
                } catch (InterruptedException e) {
                    break;
                } catch (Exception e) {
                    LOG.error(ExceptionUtils.getFullStackTrace(e));
                    LOG.trace(nodeName + ": operation failure ", e);
                }
            }
        } catch (ZkInterruptedException e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        } catch (Exception e) {
            LOG.error(ExceptionUtils.getFullStackTrace(e));
        }
        LOG.info("node operation processor for " + nodeName + " stopped");
    }
}
