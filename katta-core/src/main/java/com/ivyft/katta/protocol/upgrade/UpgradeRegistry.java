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
package com.ivyft.katta.protocol.upgrade;

import com.ivyft.katta.protocol.InteractionProtocol;
import com.ivyft.katta.protocol.metadata.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;


/**
 * <p>
 *     该类用户处理多个版本升级问题的。
 * </p>
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
public class UpgradeRegistry {


    /**
     * 该实例保存着各个版本升级的策略。
     */
    private static Map<VersionPair, UpgradeAction> _upgradeActionsByVersion = new HashMap<VersionPair, UpgradeAction>();

    //注册升级策略
    static {
        //注册一个从0.5升级到0.6版本的策略
        registerUpgradeAction("0.5", "0.6", new UpgradeAction05_06());
    }


    /**
     * log
     */
    private static final Logger LOG = LoggerFactory.getLogger(UpgradeRegistry.class);


    /**
     * 注册一个升级的策略
     * @param fromVersion 原版本
     * @param toVersion 升级后的版本
     * @param upgradeAction
     */
    protected static void registerUpgradeAction(String fromVersion,
                                                String toVersion,
                                                UpgradeAction upgradeAction) {
        _upgradeActionsByVersion.put(
                new VersionPair(fromVersion, toVersion),
                upgradeAction);
    }


    /**
     * 查找当前是否能够升级。能够升级在返回正常的UpgradeAction
     * @param protocol
     * @param distributionVersion
     * @return
     */
    public static UpgradeAction findUpgradeAction(InteractionProtocol protocol,
                                                  Version distributionVersion) {
        Version clusterVersion = protocol.getVersion();
        if (clusterVersion == null) {
            // version exist up from 0.6 only
            boolean isPre0_6Cluster = protocol.getZkClient().exists(
                    protocol.getZkConfiguration().getZkRootPath() + "/indexes");
            if (isPre0_6Cluster) {
                LOG.info("version of cluster not found - assuming 0.5");
                clusterVersion = new Version("0.5", "Unknown", "Unknown", "Unknown");
            } else {
                clusterVersion = distributionVersion;
            }
        }
        LOG.info("version of distribution " + distributionVersion.getNumber());
        LOG.info("version of cluster " + clusterVersion.getNumber());


        if (clusterVersion.equals(distributionVersion)) {
            return null;
        }

        VersionPair currentVersionPair = new VersionPair(clusterVersion.getNumber(), distributionVersion.getNumber());
        LOG.warn("cluster version differs from distribution version " + currentVersionPair);
        for (VersionPair versionPair : _upgradeActionsByVersion.keySet()) {
            LOG.info("checking upgrade action " + versionPair);
            if (currentVersionPair.getFromVersion().startsWith(versionPair.getFromVersion())
                    && currentVersionPair.getToVersion().startsWith(versionPair.getToVersion())) {
                LOG.info("found matching upgrade action");
                return _upgradeActionsByVersion.get(versionPair);
            }
        }

        LOG.warn("found no upgrade action for " + currentVersionPair + " out of " + _upgradeActionsByVersion.keySet());
        return null;
    }

}
