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

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Note: this class has a natural ordering that is inconsistent with equals.
 * <p/>
 * Sort order: score descending, doc ID ascending,
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
public class Hit implements Writable, Comparable<Hit> {

    private Text shard;

    private Text node;

    private float score;

    private int docId;

    public Hit() {
        // needed for serialization
    }


    /**
     * Construct a hit object with information about the types of the sort fields.
     */
    public Hit(String shard, String node, float score, int id) {
        this.shard = new Text(shard);
        if (node != null) {
            this.node = new Text(node);
        } else {
            this.node = null;
        }
        this.score = score;
        this.docId = id;
    }

    public Hit(Text shardName, Text serverName, float score, int docId) {
        this.shard = shardName;
        this.node = serverName;
        this.score = score;
        this.docId = docId;
    }

    public String getShard() {
        return this.shard.toString();
    }

    public String getNode() {
        return this.node.toString();
    }

    public float getScore() {
        return this.score;
    }

    public int getDocId() {
        return this.docId;
    }

    public void setDocId(int docId) {
        this.docId = docId;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.score = in.readFloat();
        final boolean hasNode = in.readBoolean();
        if (hasNode) {
            this.node = new Text();
            this.node.readFields(in);
        } else {
            this.node = null;
        }
        this.shard = new Text();
        this.shard.readFields(in);
        this.docId = in.readInt();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(this.score);
        if (this.node != null) {
            out.writeBoolean(true);
            this.node.write(out);
        } else {
            out.writeBoolean(false);
        }
        this.shard.write(out);
        out.writeInt(this.docId);
    }

    @Override
    public int compareTo(Hit o) {
        int result = Float.compare(o.score, this.score);
        if (result == 0) {
            if (this.docId < o.docId) {
                result = -1;
            } else if (this.docId > o.docId) {
                result = 1;
            } else {
                result = o.shard.compareTo(this.shard);
            }
        }
        return result;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        int temp;
        temp = Float.floatToIntBits(this.score);
        result = prime * result + temp;
        result = prime * result + ((this.node == null) ? 0 : this.node.hashCode());
        result = prime * result + ((this.shard == null) ? 0 : this.shard.hashCode());
        result = prime * result + this.docId;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        final Hit other = (Hit) obj;
        if (Float.floatToIntBits(this.score) != Float.floatToIntBits(other.score))
            return false;
        if (node == null) {
            if (other.node != null)
                return false;
        } else if (!node.equals(other.node))
            return false;
        if (shard == null) {
            if (other.shard != null)
                return false;
        } else if (!shard.equals(other.shard))
            return false;
        if (docId != other.docId)
            return false;
        return true;
    }

    @Override
    public String toString() {
        return getNode() + " " + getShard() + " " + getDocId() + " " + getScore();
    }
}
