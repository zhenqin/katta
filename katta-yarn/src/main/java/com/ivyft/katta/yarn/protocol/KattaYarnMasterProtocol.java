package com.ivyft.katta.yarn.protocol;

import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.yarn.KattaAMRMClient;
import com.ivyft.katta.yarn.KattaAppMaster;
import com.ivyft.katta.yarn.Shutdown;
import org.apache.avro.AvroRemoteException;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/11/27
 * Time: 20:59
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaYarnMasterProtocol implements KattaYarnProtocol {


    protected KattaConfiguration conf;



    protected KattaAMRMClient kattaAMRMClient;



    protected KattaAppMaster appMaster;


    private static Logger LOG = LoggerFactory.getLogger(KattaYarnMasterProtocol.class);



    public KattaYarnMasterProtocol(KattaConfiguration conf, KattaAMRMClient amrmClient) {
        this.conf = conf;
        this.kattaAMRMClient = amrmClient;
    }

    @Override
    public Void startMaster(int memory, int cores, java.lang.CharSequence kattaZip) throws AvroRemoteException {
        kattaZip = kattaZip == null ? "" : kattaZip;
        //申请 Container 内存
        appMaster.newContainer(memory, cores);
        kattaAMRMClient.startMaster(kattaZip.toString());
        return null;
    }

    @Override
    public List<KattaAndNode> listMasters() throws AvroRemoteException {
        return kattaAMRMClient.listKattaNodes(NodeType.KATTA_MASTER);
    }

    @Override
    public List<KattaAndNode> listNodes() throws AvroRemoteException {
        return kattaAMRMClient.listKattaNodes(NodeType.KATTA_NODE);
    }

    @Override
    public Void stopMaster(java.lang.CharSequence id, com.ivyft.katta.yarn.protocol.IdType idType) throws AvroRemoteException {
        try {
            ContainerId containerId = kattaAMRMClient.stop(id.toString());
            appMaster.releaseContainer(containerId);
        } catch (Exception e) {
            throw new AvroRemoteException(e);
        }
        return null;
    }

    @Override
    public Void stopAllMaster() throws AvroRemoteException {
        List<KattaAndNode> kattaAndNodes = listMasters();
        for (KattaAndNode node : kattaAndNodes) {
            stopMaster(node.getContainerId(), IdType.CONTAINER_ID);
        }
        return null;
    }

    @Override
    public Void stopNode(java.lang.CharSequence id, com.ivyft.katta.yarn.protocol.IdType idType) throws AvroRemoteException {
        try {
            ContainerId containerId = kattaAMRMClient.stop(id.toString());
            appMaster.releaseContainer(containerId);
        } catch (Exception e) {
            throw new AvroRemoteException(e);
        }
        return null;
    }

    @Override
    public Void addNode(int memory, int cores, java.lang.CharSequence kattaZip, java.lang.CharSequence solrZip) throws AvroRemoteException {
        kattaZip = kattaZip == null ? "" : kattaZip;
        //申请 Container 内存
        appMaster.newContainer(memory, cores);
        kattaAMRMClient.startNode(kattaZip.toString(), solrZip.toString());
        return null;
    }

    @Override
    public Void stopAllNode() throws AvroRemoteException {
        List<KattaAndNode> kattaAndNodes = listNodes();
        for (KattaAndNode node : kattaAndNodes) {
            stopNode(node.getContainerId(), IdType.CONTAINER_ID);
        }
        return null;
    }

    @Override
    public Void shutdown() throws AvroRemoteException {
        LOG.info("remote shutdown...");
        if(appMaster != null) {
            LOG.info("remote shutdown message...");
            appMaster.add(new Shutdown());
        }
        return null;
    }

    @Override
    public Void close() throws AvroRemoteException {
        return null;
    }


    public KattaAppMaster getAppMaster() {
        return appMaster;
    }

    public void setAppMaster(KattaAppMaster appMaster) {
        this.appMaster = appMaster;
    }
}
