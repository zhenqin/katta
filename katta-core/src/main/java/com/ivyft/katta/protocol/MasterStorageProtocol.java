package com.ivyft.katta.protocol;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
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
    Serializer serializer = new JdkSerializer();


    @Override
    public int add(Message message) throws AvroRemoteException {
        System.out.println(message.getId() + "    " + serializer.deserialize(message.getPayload().array()));
        return new Random().nextInt(10000);
    }

    @Override
    public int addList(List<Message> messages) throws AvroRemoteException {
        return 0;
    }

    @Override
    public int comm() throws AvroRemoteException {
        System.out.println("=================out===================");
        return 0;
    }

    @Override
    public int roll() throws AvroRemoteException {
        return 0;
    }

    @Override
    public int cls() throws AvroRemoteException {
        return 0;
    }
}
