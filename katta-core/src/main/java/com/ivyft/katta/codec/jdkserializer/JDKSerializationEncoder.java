package com.ivyft.katta.codec.jdkserializer;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.oneone.OneToOneEncoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;


/**
 *
 * <p>
 *     该类用JDK默认的序列化方法序列化消息。
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
public class JDKSerializationEncoder extends OneToOneEncoder {


    private JdkSerializer<Serializable> serializer = new JdkSerializer();


    /**
     * 同步
     */
    private Object monitor = new Object();


    /**
     * 日志记录
     */
    private static Logger LOG = LoggerFactory.getLogger(JDKSerializationEncoder.class);

    public JDKSerializationEncoder() {

    }

    /**
     * Transforms the specified message into another message and return the
     * transformed message.  Note that you can not return {@code null}, unlike
     * you can in {@link org.jboss.netty.handler.codec.oneone.OneToOneDecoder#decode(ChannelHandlerContext, Channel, Object)};
     * you must return something, at least {@link ChannelBuffers#EMPTY_BUFFER}.
     */
    @Override
    public Object encode(ChannelHandlerContext ctx, Channel channel, Object msg) throws Exception {
        if (msg instanceof Serializable) {
            synchronized (monitor) {
                byte [] b = serializer.serialize((Serializable)msg);
                ChannelBuffer buffer = ChannelBuffers.copiedBuffer(b);
                return buffer;
            }
        }
        return msg;
    }
}
