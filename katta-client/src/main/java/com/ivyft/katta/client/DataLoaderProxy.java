package com.ivyft.katta.client;

import com.ivyft.katta.codec.Serializer;
import com.ivyft.katta.codec.jdkserializer.JdkSerializer;
import com.ivyft.katta.protocol.Message;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * <pre>
 * Created with IntelliJ IDEA.
 * User: ZhenQin
 * Date: 2015/3/17
 * Time: 13:11
 * To change this template use File | Settings | File Templates.
 * </pre>
 *
 * @author ZhenQin
 */
public class DataLoaderProxy implements InvocationHandler {



    protected final KattaClient kattaClient;



    protected Serializer serializer = new JdkSerializer();


    public DataLoaderProxy(String host, int port) {
        this(new KattaClient(host, port));
    }


    public DataLoaderProxy(KattaClient kattaClient) {
        this.kattaClient = kattaClient;
    }



    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if(method.getName().equals("add")) {
            Pair pair = (Pair) args[0];
            Object obj = pair.getBean();
            byte[] bytes = serializer.serialize(obj);

            return kattaClient.add(new Message(pair.getShardId(), ByteBuffer.wrap(bytes)));
        } else if(method.getName().equals("addBean")) {
            String shardId = (String) args[0];
            Object obj = args[1];
            byte[] bytes = serializer.serialize(obj);

            return kattaClient.add(new Message(shardId, ByteBuffer.wrap(bytes)));
        } else if(method.getName().equals("addList")) {

            List<Pair> ds = (List<Pair>) args[0];
            List<Message> messages = new ArrayList<Message>(ds.size());
            for (Pair d : ds) {
                messages.add(new Message(d.getShardId(),
                        ByteBuffer.wrap(serializer.serialize(d.getBean()))));
            }
            return kattaClient.addList(messages);
        } else if(method.getName().equals("commit")){
            return kattaClient.commit();
        } else if(method.getName().equals("rollback")){
            return kattaClient.rollback();
        } else if(method.getName().equals("close")){
            kattaClient.close();
            return null;
        }
        throw new IllegalArgumentException("unkonwn operation.");
    }


    public KattaClient getKattaClient() {
        return kattaClient;
    }
}
