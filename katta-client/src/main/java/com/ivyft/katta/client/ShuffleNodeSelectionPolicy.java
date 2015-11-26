package com.ivyft.katta.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Node selection policy that shuffles the node list when updating, thus
 * ensuring multiple clients won't connect to the nodes in the same order.
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
public class ShuffleNodeSelectionPolicy extends BasicNodeSelectionPolicy {

    public ShuffleNodeSelectionPolicy() {

    }

    @Override
    public void update(String shard, Collection<String> nodes) {
        List<String> nodesShuffled = new ArrayList<String>(nodes);
        Collections.shuffle(nodesShuffled);
        super.update(shard, nodesShuffled);
    }
}
