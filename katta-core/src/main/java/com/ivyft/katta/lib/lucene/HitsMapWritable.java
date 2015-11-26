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
package com.ivyft.katta.lib.lucene;

import org.apache.hadoop.io.Writable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;


/**
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
public class HitsMapWritable implements Writable {

    /**
     * Shard的Node名称。 计算机名+RPC port
     */
    private String nodeName;


    /**
     * 在该Node上命中的总数
     */
    private int totalHits;


    /**
     * 命中的结果
     */
    private List<Hit> hits;


    /**
     * 查询的shard
     */
    private Set<String> shards;


    /**
     * log
     */
    private final static Logger LOG = LoggerFactory.getLogger(HitsMapWritable.class);

    public HitsMapWritable() {
        // for serialization
    }

    public HitsMapWritable(final String nodeName) {
        this.nodeName = nodeName;
        this.hits = new ArrayList<Hit>();
        this.shards = new HashSet<String>();
    }

    public void readFields(final DataInput in) throws IOException {
        long start = 0;
        if (LOG.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }
        this.nodeName = in.readUTF();
        this.totalHits = in.readInt();

        if (LOG.isDebugEnabled()) {
            LOG.debug("HitsMap reading start at: " + start + " for server " + this.nodeName);
        }
        final int shardCount = in.readInt();
        HashMap<Byte, String> shardByShardIndex = new HashMap<Byte, String>(shardCount);
        this.shards = new HashSet<String>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            String shardName = in.readUTF();
            shardByShardIndex.put((byte) i, shardName);
            this.shards.add(shardName);
        }

        final int hitCount = in.readInt();
        this.hits = new ArrayList<Hit>(hitCount + 1);
        for (int i = 0; i < hitCount; i++) {
            final byte shardIndex = in.readByte();
            final float score = in.readFloat();
            final int docId = in.readInt();
            final String shard = shardByShardIndex.get(shardIndex);
            final Hit hit;
            hit = new Hit(shard, this.nodeName, score, docId);
            addHit(hit);
        }

        if (LOG.isDebugEnabled()) {
            final long end = System.currentTimeMillis();
            LOG.debug("HitsMap reading of " + hitCount + " entries took " + (end - start) / 1000.0 + "sec.");
        }
    }

    public void write(final DataOutput out) throws IOException {
        long start = 0;
        if (LOG.isDebugEnabled()) {
            start = System.currentTimeMillis();
        }
        out.writeUTF(this.nodeName);
        out.writeInt(this.totalHits);

        int shardCount = this.shards.size();
        out.writeInt(shardCount);
        byte shardIndex = 0;
        Map<String, Byte> shardIndexByShard = new HashMap<String, Byte>(shardCount);
        for (String shard : this.shards) {
            out.writeUTF(shard);
            shardIndexByShard.put(shard, shardIndex);
            shardIndex++;
        }
        out.writeInt(this.hits.size());
        for (Hit hit : this.hits) {
            out.writeByte(shardIndexByShard.get(hit.getShard()));
            out.writeFloat(hit.getScore());
            out.writeInt(hit.getDocId());
        }
        if (LOG.isDebugEnabled()) {
            final long end = System.currentTimeMillis();
            LOG.debug("HitsMap writing took " + (end - start) + "ms.");
            LOG.debug("HitsMap writing ended at: " + end + " for server " + this.nodeName);
        }
    }

    public void addHit(final Hit hit) {
        this.hits.add(hit);
        this.shards.add(hit.getShard());
    }

    /**
     * @deprecated use {@link #addHit(Hit)} instead
     */
    public void addHitToShard(final String shard, final Hit hit) {
        addHit(hit);
    }

    /**
     * @deprecated use {@link #getNodeName()} instead
     */
    public String getServerName() {
        return getNodeName();
    }

    public String getNodeName() {
        return this.nodeName;
    }

    public List<Hit> getHitList() {
        return this.hits;
    }

    /**
     * @deprecated use {@link #getHitList()} instead
     */
    public Hits getHits() {
        final Hits result = new Hits();
        result.setTotalHits(this.totalHits);
        result.addHits(this.hits);
        return result;
    }

    public void addTotalHits(final int length) {
        this.totalHits += length;
    }

    public int getTotalHits() {
        return this.totalHits;
    }
}
