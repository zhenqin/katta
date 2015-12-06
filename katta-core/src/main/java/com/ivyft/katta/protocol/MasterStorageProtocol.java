package com.ivyft.katta.protocol;

import com.ivyft.katta.lib.writer.DataWriter;
import com.ivyft.katta.util.MasterConfiguration;
import org.apache.avro.AvroRemoteException;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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

    protected final InteractionProtocol protocol;


    protected final MasterConfiguration conf;


    protected final Map<String, DataWriter> CACHED_INDEX_DATAWRITER_MAP = new ConcurrentHashMap<String, DataWriter>(3);


    protected final Class<? extends DataWriter> aClass;



    protected final Set<String> indices = new HashSet<String>(3);



    public MasterStorageProtocol(MasterConfiguration conf, InteractionProtocol protocol) {
        this.conf = conf;
        this.protocol = protocol;

        this.aClass = (Class<? extends DataWriter>) conf.getClass("master.data.writer");

        indices.addAll(protocol.getIndices());

    }



    private DataWriter getDataWriter(String index) {
        DataWriter dataWriter = CACHED_INDEX_DATAWRITER_MAP.get(index);
        if(dataWriter == null) {
            if(!indices.contains(index)) {
                throw new IllegalArgumentException("没有索引集: " + index);
            }
            try {
                dataWriter = aClass.newInstance();
                dataWriter.init(conf, protocol, index);
                CACHED_INDEX_DATAWRITER_MAP.put(index, dataWriter);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }

        return dataWriter;
    }



    @Override
    public int add(Message message) throws AvroRemoteException {
        getDataWriter(message.getIndexId().toString()).write(message.getRowId().toString(), message.getPayload());
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
    public Void comm(java.lang.CharSequence indexId) throws AvroRemoteException {
        System.out.println("=================out===================");
        try {
            getDataWriter(indexId.toString()).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public Void roll(java.lang.CharSequence indexId) throws AvroRemoteException {
        return null;
    }

    @Override
    public Void cls() throws AvroRemoteException {
        return null;
    }
}
