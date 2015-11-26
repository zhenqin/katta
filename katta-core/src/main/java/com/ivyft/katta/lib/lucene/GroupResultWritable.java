package com.ivyft.katta.lib.lucene;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

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
public class GroupResultWritable<W extends Writable> implements Writable, Set<W> {



    /**
     * 命中稳定的最大评分
     */
    protected float maxScore;


    /**
     * 命中的文档数量
     */
    protected int totalHitCount;

    /**
     *
     * 保存Map的实例
     */
    private Set<W> instance;



    /**
     * key的临时变量
     */
    private W w;

    /**
     * default constructor,Hadoop RPC use it
     */
    public GroupResultWritable() {
        this.instance = new HashSet<W>();
    }


    public GroupResultWritable(W w) {
        this.w = w;
        if(w == null) {
            throw new IllegalArgumentException("tempW must be not null.");
        }
        this.instance = new HashSet<W>();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeFloat(maxScore);
        out.writeInt(totalHitCount);

        String clazzString = w.getClass().getName();
        out.writeInt(clazzString.length());
        out.writeBytes(clazzString);

        // Write out the number of entries in the map
        out.writeInt(instance.size());

        // Then write out each key/value pair
        for (W w: instance) {
            w.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        maxScore = in.readFloat();
        totalHitCount = in.readInt();

        // First clear the map.  Otherwise we will just accumulate
        // entries every time this method is called.
        this.instance.clear();
        int length = in.readInt();

        byte[] b = new byte[length];
        in.readFully(b);
        String clazzString = new String(b);

        int entries = in.readInt();
        // Then read each key/value pair

        Class<W> clazz = null;
        try {
            clazz = (Class<W>) Class.forName(clazzString);
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        try {
            for (int i = 0; i < entries; i++) {
                w = clazz.newInstance();
                w.readFields(in);
                instance.add(w);
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }



    public Collection<W> get() {
        return instance;
    }


    @Override
    public void clear() {
        instance.clear();
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
    public boolean contains(Object o) {
        return instance.contains(o);
    }

    @Override
    public Iterator<W> iterator() {
        return instance.iterator();
    }

    @Override
    public Object[] toArray() {
        return instance.toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return instance.toArray(a);
    }

    public boolean add(W w) {
        if(w != null) {
            return instance.add(w);
        }
        return false;
    }

    @Override
    public boolean remove(Object o) {
        return instance.remove(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return instance.containsAll(c);
    }

    public boolean addAll(Collection<? extends W> c) {
        return instance.addAll(c);
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        return instance.retainAll(c);
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        return instance.removeAll(c);
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
}
