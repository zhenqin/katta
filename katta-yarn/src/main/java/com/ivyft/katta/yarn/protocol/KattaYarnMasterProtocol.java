package com.ivyft.katta.yarn.protocol;

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


    public KattaYarnMasterProtocol() {

    }

    @Override
    public Void startMaster(int num) throws AvroRemoteException {
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
        return null;
    }

    @Override
    public Void stopAllNode() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void shutdown() throws AvroRemoteException {
        return null;
    }
}
