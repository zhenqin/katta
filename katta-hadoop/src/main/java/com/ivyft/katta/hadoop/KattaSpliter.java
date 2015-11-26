package com.ivyft.katta.hadoop;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.protocol.metadata.Shard;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-4-4
 * Time: 上午11:51
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaSpliter {


    /**
     * log
     */
    private static Logger log = LoggerFactory.getLogger(KattaSpliter.class);


    /**
     * 计算当前节点
     * @param configuration 配置
     * @return
     */
    public static List<KattaInputSplit> calculateSplits(Configuration configuration) {
        List<KattaInputSplit> inputSplits = new ArrayList<KattaInputSplit>();
        String[] indexes = KattaInputFormat.getIndexNames(configuration);
        if(indexes.length == 0) {
            throw new IllegalArgumentException("indexes must not empty.");
        }

        ZkClient zkClient = new ZkClient(configuration.get("zookeeper.servers", "localhost"),
                configuration.getInt("zookeeper.tick-time", 6000),
                configuration.getInt("zookeeper.timeout", 60000));


        Properties prop = new Properties();
        for (Map.Entry<String, String> entry : configuration) {
            prop.setProperty(entry.getKey(), entry.getValue());
        }

        ZkConfiguration zkConf = new ZkConfiguration(prop, null);
        InteractionProtocol protocol = new InteractionProtocol(zkClient, zkConf);

        List<String> selectIndex = null;
        if(indexes.length == 1 && StringUtils.equals(indexes[0], "*")) {
            selectIndex = protocol.getIndices();
        } else {
            selectIndex = Arrays.asList(indexes);
        }

        for(String index : selectIndex) {
            IndexMetaData indexMetaData = protocol.getIndexMD(index);
            if(indexMetaData == null) {
                throw new IllegalArgumentException("找不到Index: " + index + " 的ZooKeeper注册信息。");
            }

            Set<Shard> shardList = indexMetaData.getShards();
            for (Shard shard : shardList) {
                List<String> installNodes = protocol.getShardNodes(shard.getName());
                if(installNodes == null || installNodes.isEmpty()) {
                    throw new IllegalStateException(shard.getName() + " no node to install.");
                }
                String node = randomNode(installNodes);
                KattaInputSplit split = new KattaInputSplit();
                split.setPort(KattaInputFormat.getSocketPort(configuration));
                split.setKeyField(KattaInputFormat.getInputKey(configuration));
                split.setQuery(KattaInputFormat.getInputQuery(configuration));
                split.setLimit(KattaInputFormat.getLimit(configuration));

                split.setHost(node.substring(0, node.indexOf(":")));
                log.info("katta reader multi thread: " + split.getHost() + "    " + shard.getName());
                split.setShardName(shard.getName());

                inputSplits.add(split);
            }
        }

        return inputSplits;
    }



    public static String randomNode(List<String> nodes) {
        Random random = new Random();
        return nodes.get(random.nextInt(nodes.size()));
    }


    public static void main(String[] args) {
        List<String> list = Arrays.asList("11", "44");
        for (int i = 0; i < 10; i++) {
            System.out.println(randomNode(list));
        }
    }
}
