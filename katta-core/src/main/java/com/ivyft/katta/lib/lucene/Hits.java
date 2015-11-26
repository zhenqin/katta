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

import com.ivyft.katta.util.MergeSort;
import com.ivyft.katta.util.WritableType;
import org.apache.hadoop.io.Writable;
import org.apache.lucene.search.Sort;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;


/**
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
public class Hits implements Writable {


    private List<List<Hit>> hitsList = new Vector<List<Hit>>();

    private List<Hit> sortedList;

    private AtomicInteger totalHits = new AtomicInteger();

    private Set<String> missingShards = new HashSet<String>();


    public Hits() {
    }



    public void readFields(final DataInput in) throws IOException {
        // final long start = System.currentTimeMillis();
        final int listOfListsSize = in.readInt();
        hitsList = new ArrayList<List<Hit>>(listOfListsSize);
        for (int i = 0; i < listOfListsSize; i++) {
            final int hitSize = in.readInt();
            final List<Hit> hitList = new ArrayList<Hit>(hitSize);
            for (int j = 0; j < hitSize; j++) {
                final Hit hit = new Hit();
                hit.readFields(in);
                hitList.add(hit);
            }
            hitsList.add(hitList);

        }
        // final long end = System.currentTimeMillis();
        // Logger.info("Hits reading took " + (end - start) / 1000.0 + "sec.");
    }

    public void write(final DataOutput out) throws IOException {
        // final long start = System.currentTimeMillis();
        out.writeInt(hitsList.size());
        for (final List<Hit> hitList : hitsList) {
            out.writeInt(hitList.size());
            for (final Hit hit : hitList) {
                hit.write(out);
            }
        }
        // final long end = System.currentTimeMillis();
        // Logger.info("Hits writing took " + (end - start) / 1000.0 + "sec.");
    }

    public int size() {
        return totalHits.get();
    }


    public int numFound() {
        return totalHits.get();
    }


    public List<Hit> getHits() {
        if (sortedList == null) {
            sort(Integer.MAX_VALUE);
        }
        return sortedList;
    }

    public void addHits(final List<Hit> hits) {
        hitsList.add(hits);
    }

    public void setTotalHits(int totalHits) {
        this.totalHits.set(totalHits);
    }


    public void sort(final int count) {
        sortCollection(count);
    }

    public void fieldSort(Sort sort, WritableType[] fieldTypes, int count) {
        final ArrayList<Hit> list = new ArrayList<Hit>(count);
        final int size = hitsList.size();
        for (int i = 0; i < size; i++) {
            list.addAll(hitsList.remove(0));
        }
        hitsList = new ArrayList<List<Hit>>();
        if (!list.isEmpty()) {
            Collections.sort(list, new FieldSortComparator(sort.getSort(), fieldTypes));
        }
        sortedList = list.subList(0, Math.min(count, list.size()));
    }


    public void sortMerge() {
        final List<Hit>[] array = hitsList.toArray(new List[hitsList.size()]);
        hitsList = new ArrayList<List<Hit>>();
        sortedList = MergeSort.merge(array);
    }

    /*
     * Leads to OOM on 2 000 000 elements.
     */
    public void sortOther() {
        sortedList = new ArrayList<Hit>();
        while (true) {
            Hit highest = null;
            final int[] pos = new int[hitsList.size()];
            for (int i = 0; i < pos.length; i++) {
                pos[i] = 0;
            }
            int pointer = 0;
            for (int i = 0; i < hitsList.size(); i++) {
                final List<Hit> list = hitsList.get(i);
                if (list.size() > pos[i]) {
                    final Hit hit = list.get(pos[i]);
                    if (highest == null) {
                        highest = hit;
                        pointer = i;
                    } else if (hit.getScore() > highest.getScore()) {
                        highest = hit;
                        pointer = i;
                    }
                }
            }
            if (highest == null) {
                // no data anymore
                return;
            }
            pos[pointer]++;
            sortedList.add(highest);
            highest = null;
        }
    }

    public void sortOtherII() {
        sortedList = new ArrayList<Hit>();
        int pos = 0;
        while (true) {
            final List<Hit> tmp = new ArrayList<Hit>(hitsList.size());
            for (final List<Hit> hitList : hitsList) {
                if (hitList.size() > pos) {
                    tmp.add(hitList.get(pos));
                }
            }
            if (tmp.size() == 0) {
                // we are done no new data
                return;
            }
            Collections.sort(tmp);
            sortedList.addAll(tmp);
            pos++;
        }
    }

    /*
     * Leads on 10 000 000 list to OOM.
     */
    public void sortCollection(final int count) {
        final ArrayList<Hit> list = new ArrayList<Hit>();
        final int size = hitsList.size();
        for (int i = 0; i < size; i++) {
            list.addAll(hitsList.remove(0));
        }
        hitsList = new ArrayList<List<Hit>>();
        Collections.sort(list);
        sortedList = list.subList(0, Math.min(count, list.size()));
    }

    // public int compare(Hit o1, Hit o2) {
    // final float score2 = o2.getScore();
    // final float score1 = o1.getScore();
    // if (score1 > score2) {
    // return 1;
    // }
    // return -1;
    // }

    public void addTotalHits(int size) {
        totalHits.addAndGet(size);
    }


    public void addMissingShards(Set<String> missingShards) {
        this.missingShards.addAll(missingShards);
    }

    @Override
    public String toString() {
    /*
     * Don't modify data structure just by viewing it, otherwise
     * running in a debugger modifies the behavior of the code!
     */
        return "Hits: total=" + totalHits + ", queue=" + (hitsList != null ? hitsList.toString() : "null") +
                ", sorted=" + (sortedList != null ? sortedList.toString() : "null");
    }

    public Set<String> getMissingShards() {
        return missingShards;
    }

    public void setMissingShards(Set<String> _missingShards) {
        this.missingShards = _missingShards;
    }

}
