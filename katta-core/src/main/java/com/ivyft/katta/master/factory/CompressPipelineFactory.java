package com.ivyft.katta.master.factory;

import com.ivyft.katta.codec.jdkserializer.JDKSerializationDecoder;
import org.apache.solr.core.SolrCore;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;

/**
 *
 * <p>
 *     压缩和解压缩[ZlibEncoder，ZlibDecoder]应该每次都申请新的对象。否则会发生错误
 * </p>
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-5-31
 * Time: 上午11:21
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class CompressPipelineFactory implements ChannelPipelineFactory {


    private SolrCore core;


    public CompressPipelineFactory(SolrCore core) {
        this.core = core;
    }


    @Override
    public ChannelPipeline getPipeline() throws Exception {
        ChannelPipeline pipeline = Channels.pipeline();

        pipeline.addLast("headerDecoding", new LengthFieldBasedFrameDecoder(Integer.MAX_VALUE, 0, 4, 0, 4));
        pipeline.addLast("decoder", new JDKSerializationDecoder());

        pipeline.addLast("headerEncoding", new LengthFieldPrepender(4));
        //pipeline.addLast("compressEncoding", new ZlibEncoder());
        //pipeline.addLast("encoder", new JDKSerializationEncoder());

        //pipeline.addLast("handler", new KattaExportMetaHandler(core));

        return pipeline;
    }

}
