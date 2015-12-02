package com.ivyft.katta.yarn.protocol;

import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.yarn.KattaAMRMClient;
import com.ivyft.katta.yarn.KattaAppMaster;
import com.ivyft.katta.yarn.Shutdown;
import org.apache.avro.AvroRemoteException;

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



    protected KattaAMRMClient amrmClient;



    protected KattaAppMaster appMaster;


    public KattaYarnMasterProtocol(KattaConfiguration conf, KattaAMRMClient amrmClient) {
        this.conf = conf;
        this.amrmClient = amrmClient;
    }

    @Override
    public Void startMaster(int num) throws AvroRemoteException {
        amrmClient.startMaster();
        return null;
    }

    @Override
    public Void stopMaster() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void startNode(int num) throws AvroRemoteException {
        return null;
    }

    @Override
    public Void addNode(int num) throws AvroRemoteException {
        amrmClient.startNode();
        return null;
    }

    @Override
    public Void stopAllNode() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void shutdown() throws AvroRemoteException {
        if(appMaster != null) {
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
