package com.ivyft.katta.lib.lucene;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-1-9
 * Time: 下午3:41
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ArrayMapWritable implements Writable, Iterable<MapWritable> {


    private LinkedList<MapWritable> mapWritableList = new LinkedList<MapWritable>();


    public ArrayMapWritable() {

    }


    public void push(MapWritable mapWritable) {
        mapWritableList.push(mapWritable);
    }

    public MapWritable pop() {
        return mapWritableList.pop();
    }


    public int size() {
        return mapWritableList.size();
    }

    public boolean add(MapWritable mapWritable) {
        return mapWritableList.add(mapWritable);
    }

    public boolean remove(Object o) {
        return mapWritableList.remove(o);
    }

    public void clear() {
        mapWritableList.clear();
    }

    public MapWritable get(int index) {
        return mapWritableList.get(index);
    }

    public boolean addAll(int index, Collection<? extends MapWritable> c) {
        return mapWritableList.addAll(index, c);
    }


    public boolean addAll(ArrayMapWritable list) {
        return mapWritableList.addAll(list.get());
    }


    public List<MapWritable> get() {
        return this.mapWritableList;
    }


    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(mapWritableList.size());
        for (MapWritable writable : mapWritableList) {
            writable.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int length = in.readInt();
        for(int i = 0; i < length; i++) {
            MapWritable map = new MapWritable();
            map.readFields(in);
            mapWritableList.add(map);
        }
    }

    @Override
    public Iterator<MapWritable> iterator() {
        return mapWritableList.iterator();
    }
}
