package com.ivyft.katta.codec.jdkserializer;

import com.ivyft.katta.serializer.jdkserializer.JdkSerializer;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 *
 * <p>
 *     该类用JDK默认的序列化方法反序列化消息。
 *     因此所有的消息都必须是Serializable的子类
 * </p>
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-6-3
 * Time: 上午10:55
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class JDKSerializationDecoder extends OneToOneDecoder {


    private JdkSerializer<Serializable> serializer = new JdkSerializer();

    /**
     * 同步
     */
    private Object monitor = new Object();



    /**
     * 日志记录
     */
    private static Logger LOG = LoggerFactory.getLogger(JDKSerializationDecoder.class);

    public JDKSerializationDecoder() {

    }

    /**
     * Transforms the specified received message into another message and return
     * the transformed message.  Return {@code null} if the received message
     * is supposed to be discarded.
     */
    @Override
    public Object decode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        if (!(msg instanceof ChannelBuffer)) {
            return msg;
        }
        synchronized (monitor) {
            ChannelBuffer serial = (ChannelBuffer) msg;
            byte[] in = new byte[serial.readableBytes()];
            serial.readBytes(in);
            return serializer.deserialize(in);
        }
    }
}
