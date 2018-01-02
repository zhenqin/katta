package com.ivyft.katta;

import com.ivyft.katta.node.IndexUpdateListener;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.tool.index.IndexMergeTool;
import com.ivyft.katta.util.NodeConfiguration;
import com.ivyft.katta.util.ZkConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

import java.net.URI;

/**
 * <pre>
 *
 * Created by zhenqin.
 * User: zhenqin
 * Date: 2017/12/28
 * Time: 14:56
 * Vendor: yiidata.com
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class MergeIndex extends ProtocolCommand implements IndexUpdateListener {


    protected int segment = 2;



    protected boolean delete = false;


    protected URI to = null;


    protected URI from = null;


    public MergeIndex() {
        super("mergeIndex", "Merge mutil hdfs|file lucene index dir to one index.");
    }

    @Override
    protected void execute(ZkConfiguration zkConf, InteractionProtocol protocol) throws Exception {
        IndexMergeTool tool = new IndexMergeTool(new NodeConfiguration(), this);
        if (from == null) {
            // 如果只有 to，则表明只是优化索引
            tool.mergeIndex(segment, delete, new URI[]{to});
        } else {
            tool.mergeIndex(segment, delete, new URI[]{to, from});
        }
    }

    @Override
    public Options getOpts() {
        Options options = new Options();
        options.addOption("t", "to", true, "merge lucene index to ?");
        options.addOption("f", "from", true, "merge lucene index from ?");
        options.addOption("S", "segment", true, "Lucene Index optimize {num} Segment");
        options.addOption("D", "delete", false, "Merge Index success, delete from index?");
        options.addOption("s", false, "print exception");
        return options;
    }

    @Override
    public void process(CommandLine cl) throws Exception {
        String to = cl.getOptionValue("t");
        if(StringUtils.isBlank(to)){
            throw new IllegalArgumentException("from path and to path must not be blank.");
        }

        this.to = new URI(to);

        String from = cl.getOptionValue("f");
        if (StringUtils.isNotBlank(from)) {
            this.from = new URI(from);
        }

        if(cl.hasOption("S")) {
            segment = Integer.parseInt(cl.getOptionValue("S"));
        }

        if(cl.hasOption("D")) {
            delete = true;
        }

    }

    @Override
    public void onBeforeUpdate(String indexName, String shardName) {

    }

    @Override
    public void onAfterUpdate(String indexName, String shardName) {
        System.out.println("索引将要关闭。");
    }
}
