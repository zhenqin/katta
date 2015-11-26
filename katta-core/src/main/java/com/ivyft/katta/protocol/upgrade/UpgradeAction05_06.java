/**
 * Copyright 2008 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ivyft.katta.protocol.upgrade;

import com.ivyft.katta.operation.master.IndexReinitializeOperation;
import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.IndexMetaData;
import com.ivyft.katta.util.ZkConfiguration;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-13
 * Time: 上午8:58
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
class UpgradeAction05_06 implements UpgradeAction {

    private static final Logger LOG = LoggerFactory.getLogger(UpgradeAction05_06.class);


    UpgradeAction05_06() {

    }

    @Override
    public void upgrade(InteractionProtocol protocol) {
        ZkClient zkClient = protocol.getZkClient();

        ZkConfiguration zkConf = protocol.getZkConfiguration();

        ZkClient zkClientForWriables = new ZkClient(zkConf.getZKServers(), 5000, 5000, new WriteableZkSerializer(
                com.ivyft.katta.index.IndexMetaData.class));
        LOG.info("restoring indices meta data...");
        String oldIndicesPath = getOldIndicesPath(zkConf);
        List<String> indices = zkClient.getChildren(oldIndicesPath);

        LOG.info("found " + indices.size() + " old indices");

        for (String indexName : indices) {
            IndexMetaData oldIndexMD = zkClientForWriables.readData(oldIndicesPath + "/" + indexName);


            IndexMetaData newIndexMD = new IndexMetaData(indexName,
                    oldIndexMD.getPath(),
                    oldIndexMD.getCollectionName(),
                    oldIndexMD.getReplicationLevel());

            IndexReinitializeOperation deployOperation = new IndexReinitializeOperation(newIndexMD);
            protocol.addMasterOperation(deployOperation);
            protocol.publishIndex(newIndexMD);
        }
        zkClientForWriables.close();

        LOG.info("deleting obsolete folders...");
        zkClient.deleteRecursive(zkConf.getZkRootPath() + "/" + "indexes");
        zkClient.deleteRecursive(zkConf.getZkRootPath() + "/" + "node-to-shard");
        zkClient.deleteRecursive(zkConf.getZkRootPath() + "/" + "shard-to-node");
        zkClient.deleteRecursive(zkConf.getZkRootPath() + "/" + "loadtest-nodes");
        zkClient.deleteRecursive(zkConf.getZkRootPath() + "/" + "server-metrics");
        zkClient.deleteRecursive(zkConf.getZkRootPath() + "/" + "shard-to-error");

        LOG.info("upgrade done");
    }

    protected static String getOldIndicesPath(ZkConfiguration zkConf) {
        return zkConf.getZkRootPath() + "/" + "indexes";
    }

    static class WriteableZkSerializer implements ZkSerializer {

        private final Class<? extends Writable> _writableClass;

        public WriteableZkSerializer(Class<? extends Writable> writableClass) {
            _writableClass = writableClass;
        }

        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            DataInputBuffer buffer = new DataInputBuffer();
            buffer.reset(bytes, bytes.length);
            try {
                Writable instance = _writableClass.newInstance();
                instance.readFields(buffer);
                buffer.close();
                return instance;
            } catch (Exception e) {
                throw new ZkMarshallingError(e);
            }
        }

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            DataOutputBuffer out = new DataOutputBuffer();
            try {
                ((Writable) data).write(out);
                return out.getData();
            } catch (Exception e) {
                throw new ZkMarshallingError(e);
            } finally {
                IOUtils.closeStream(out);
            }
        }

    }

}
