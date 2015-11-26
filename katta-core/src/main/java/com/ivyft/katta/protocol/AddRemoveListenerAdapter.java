package com.ivyft.katta.protocol;

import com.ivyft.katta.util.CollectionUtil;
import org.I0Itec.zkclient.IZkChildListener;

import java.util.Collections;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午10:07
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class AddRemoveListenerAdapter  extends ListenerAdapter implements IZkChildListener {

    private List<String> cachedChilds;


    private final IAddRemoveListener listener;

    public AddRemoveListenerAdapter(String path, IAddRemoveListener listener) {
        super(path);
        this.listener = listener;
    }

    public void setCachedChilds(List<String> cachedChilds) {
        this.cachedChilds = cachedChilds;
        if (this.cachedChilds == null) {
            this.cachedChilds = Collections.emptyList();
        }
    }

    public List<String> getCachedChilds() {
        return this.cachedChilds;
    }

    @Override
    public synchronized void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
        if (currentChilds == null) {
            currentChilds = Collections.emptyList();
        }
        List<String> addedChilds = CollectionUtil.getListOfAdded(this.cachedChilds, currentChilds);
        List<String> removedChilds = CollectionUtil.getListOfRemoved(this.cachedChilds, currentChilds);
        for (String addedChild : addedChilds) {
            this.listener.added(addedChild);
        }
        for (String removedChild : removedChilds) {
            this.listener.removed(removedChild);
        }
        this.cachedChilds = currentChilds;
    }
}
