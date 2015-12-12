package com.ivyft.katta.yarn.protocol;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.yarn.KattaAMRMClient;
import com.ivyft.katta.yarn.KattaAppMaster;
import com.ivyft.katta.yarn.Shutdown;
import org.apache.avro.AvroRemoteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

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


    /**
     *
     *
     */
    private SetMultimap<String, String> aliveKattaMasterMap = HashMultimap.create();



    /**
     *
     *
     */
    private SetMultimap<String, String> aliveKattaNodeMap = HashMultimap.create();




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
    public Void registerMaster(KattaAndNode kattaNode) throws AvroRemoteException {
        aliveKattaMasterMap.put("", "");
        return null;
    }

    @Override
    public List<KattaAndNode> listMasters() throws AvroRemoteException {
        Set<String> strings = aliveKattaMasterMap.keySet();
        return null;
    }

    @Override
    public Void unregisterMaster(KattaAndNode kattaNode) throws AvroRemoteException {
        return null;
    }

    @Override
    public Void registerNode(KattaAndNode kattaNode) throws AvroRemoteException {
        return null;
    }

    @Override
    public Void unregisterNode(KattaAndNode kattaNode) throws AvroRemoteException {
        return null;
    }

    @Override
    public List<KattaAndNode> listNodes() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void stopMaster() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void stopNode() throws AvroRemoteException {
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
