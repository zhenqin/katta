/**
 * Copyright 2009 the original author or authors.
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
package com.ivyft.katta.lib.mapfile;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.ipc.VersionedProtocol;

import java.io.IOException;

/**
 * Interface for the client calls that will arrive via Hadoop RPC.
 * <p/>
 * This server looks up Text entries from MapFiles using Text keys.
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
public interface IMapFileServer extends VersionedProtocol {

    /**
     * Get all the occurrences of the given Text key. There could be
     * up to one entry per shard.
     *
     * @param key    The key to search for.
     * @param shards Which MapFile shards to look in.
     * @return The list of Text results.
     * @throws IOException If an error occurs.
     */
    public TextArrayWritable get(Text key, String[] shards) throws IOException;

}
