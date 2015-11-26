package com.ivyft.katta;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-3-22
 * Time: 下午1:43
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class CounterMap<K> {

    private Map<K, AtomicInteger> counterMap = new HashMap<K, AtomicInteger>();

    public CounterMap() {
        super();
    }

    public void increment(K key) {
        AtomicInteger integer = counterMap.get(key);
        if (integer == null) {
            integer = new AtomicInteger(0);
            counterMap.put(key, integer);
        }
        integer.incrementAndGet();
    }

    public int getCount(K key) {
        AtomicInteger integer = counterMap.get(key);
        if (integer == null) {
            return 0;
        }
        return integer.get();
    }

    public Set<K> keySet() {
        return counterMap.keySet();
    }
}
