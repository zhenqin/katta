package com.ivyft.katta.lib.lucene;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午9:27
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class FacetResultWritable<W extends Writable> implements Writable, Map<W, IntWritable> {


    /**
     *
     * 保存Map的实例
     */
    private Map<W, IntWritable> instance;


    /**
     * key的临时变量
     */
    private W w;



    /**
     * 命中稳定的最大评分
     */
    protected float maxScore = 0.0f;


    /**
     * 命中的文档数量
     */
    protected int totalHitCount = 0;


    /**
     * Group命中文档的最大数量
     */
    protected int totalGroupedHitCount = 0;



    /**
     * default constructor
     */
    public FacetResultWritable() {
        this.instance = new HashMap<W, IntWritable>();
    }

    /**
     *
     */
    public FacetResultWritable(W tempW) {
        this.w = tempW;
        if(w == null) {
            throw new IllegalArgumentException("tempW must be not null.");
        }
        this.instance = new HashMap<W, IntWritable>();
    }


    @Override
    public void write(DataOutput out) throws IOException {
        // Write out the number of entries in the map
        out.writeInt(totalHitCount);
        out.writeInt(totalGroupedHitCount);
        out.writeFloat(maxScore);


        out.writeUTF(w.getClass().getName());
        out.writeInt(instance.size());
        // Then write out each key/value pair

        for (Entry<W, IntWritable> e: instance.entrySet()) {
            e.getKey().write(out);
            e.getValue().write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        // First clear the map.  Otherwise we will just accumulate
        // entries every time this method is called.
        this.instance.clear();


        totalHitCount = in.readInt();
        totalGroupedHitCount = in.readInt();
        maxScore = in.readFloat();

        Class<W> clazz = null;
        try {
            String clazzString = in.readUTF();
            clazz = (Class<W>) Class.forName(clazzString);
        } catch (Exception e) {
            throw new IOException(e);
        }

        // Read the number of entries in the map
        int entries = in.readInt();
        // Then read each key/value pair

        try {
            for (int i = 0; i < entries; i++) {
                w = clazz.newInstance();
                w.readFields(in);

                IntWritable value = new IntWritable();
                value.readFields(in);
                instance.put(w, value);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }


    public Map<W, IntWritable> get() {
        return instance;
    }


    @Override
    public int size() {
        return instance.size();
    }

    @Override
    public boolean isEmpty() {
        return instance.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return instance.containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return instance.containsKey(value);
    }

    @Override
    public IntWritable get(Object key) {
        return instance.get(key);
    }

    @Override
    public IntWritable put(W key, IntWritable value) {
        return instance.put(key, value);
    }

    @Override
    public IntWritable remove(Object key) {
        return instance.remove(key);
    }

    @Override
    public void putAll(Map<? extends W, ? extends IntWritable> m) {
        for (Entry<? extends Writable, ? extends IntWritable> e: m.entrySet()) {
            put((W) e.getKey(), e.getValue());
        }
    }


    public void addAll(Map<? extends W, ? extends AtomicInteger> m) {
        for (Entry<? extends W, ? extends AtomicInteger> e: m.entrySet()) {
            IntWritable value = instance.get(e.getKey());
            if(value != null) {
                //累加这个值
                value.set(e.getValue().addAndGet(value.get()));
            } else {
                //初始这个值，并Put放入Map，下一次则是累加
                value = new IntWritable(e.getValue().intValue());
                instance.put(e.getKey(), value);
            }
        }
    }


    @Override
    public void clear() {
        instance.clear();
    }

    @Override
    public Set<W> keySet() {
        return instance.keySet();
    }

    @Override
    public Collection<IntWritable> values() {
        return instance.values();
    }

    @Override
    public Set<Entry<W, IntWritable>> entrySet() {
        return instance.entrySet();
    }


    public float getMaxScore() {
        return maxScore;
    }

    public void setMaxScore(float maxScore) {
        this.maxScore = Math.max(this.maxScore, maxScore);
    }

    public int getTotalHitCount() {
        return totalHitCount;
    }

    public void addTotalHitCount(int totalHitCount) {
        this.totalHitCount += totalHitCount;
    }

    public int getTotalGroupedHitCount() {
        return totalGroupedHitCount;
    }

    public void addTotalGroupedHitCount(int totalGroupedHitCount) {
        this.totalGroupedHitCount += totalGroupedHitCount;
    }

    public int getTotalGroupCount() {
        return instance.size();
    }
}
