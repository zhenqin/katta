package com.ivyft.katta.protocol;

import com.ivyft.katta.lib.writer.DataWriter;
import com.ivyft.katta.util.MasterConfiguration;
import org.apache.avro.AvroRemoteException;

import java.util.List;
import java.util.Random;

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
public class MasterStorageProtocol implements KattaClientProtocol {

    protected InteractionProtocol protocol;


    protected MasterConfiguration conf;
    protected DataWriter writer;


    public MasterStorageProtocol(MasterConfiguration conf, InteractionProtocol protocol) {
        this.conf = conf;
        this.protocol = protocol;

        try {
            Class<? extends DataWriter> aClass = (Class<? extends DataWriter>) conf.getClass("master.data.writer");
            this.writer = aClass.newInstance();
            this.writer.init(conf, protocol);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public int add(Message message) throws AvroRemoteException {
        this.writer.write(message.getRowId().toString(), message.getPayload());
        return new Random().nextInt(10000);
    }

    @Override
    public int addList(List<Message> messages) throws AvroRemoteException {
        int count = 0;
        for (Message message : messages) {
            count += this.add(message);
        }
        return count;
    }

    @Override
    public Void comm() throws AvroRemoteException {
        System.out.println("=================out===================");
        try {
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Void roll() throws AvroRemoteException {
        return null;
    }

    @Override
    public Void cls() throws AvroRemoteException {
        return null;
    }
}
