package com.ivyft.katta.lib.lucene;

import org.apache.lucene.util.PriorityQueue;

import java.util.Iterator;

/**
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午9:15
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaHitQueue  extends PriorityQueue<Hit> implements Iterable<Hit> {

    private final int _maxSize;

    public KattaHitQueue(int maxSize) {
        super(maxSize);
        this._maxSize = maxSize;
    }


    public boolean insert(Hit hit) {
        if (size() < _maxSize) {
            add(hit);
            return true;
        }
        if (lessThan(top(), hit)) {
            insertWithOverflow(hit);
            return true;
        }
        return false;
    }

    @Override
    protected final boolean lessThan(final Hit hitA, final Hit hitB) {
        return hitA.compareTo(hitB) > 0;
    }

    @Override
    public Iterator<Hit> iterator() {
        return new Iterator<Hit>() {
            @Override
            public boolean hasNext() {
                return KattaHitQueue.this.size() > 0;
            }

            @Override
            public Hit next() {
                return KattaHitQueue.this.pop();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException("Can't remove using this iterator");
            }
        };
    }
}
