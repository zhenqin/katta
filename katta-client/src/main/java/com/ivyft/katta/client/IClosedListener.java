package com.ivyft.katta.client;

/**
 *
 *
 * Provides a way to notify interested parties when our close() method is
 * called.
 *
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-12-20
 * Time: 上午10:24
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public interface IClosedListener {


    /**
     * The ClientResult's close() method was called. The result is closed before
     * calling this.
     */
    public void clientResultClosed();
}
