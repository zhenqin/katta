package com.ivyft.katta.protocol;

import com.ivyft.katta.util.KattaConfiguration;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/11
 * Time: 15:07
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public interface KattaRegister {



    public void init(KattaConfiguration conf);


    public void register(String host, String hostCode, String nodeType, String appAttemptId);


}
