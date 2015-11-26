package com.ivyft.katta.protocol.upgrade;

/**
 *
 * 用该类来判断是否能够升级
 *
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 13-11-19
 * Time: 上午10:18
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class VersionPair {


    /**
     * 升级时原来的版本
     */
    private String fromVersion;


    /**
     * 升级后的版本
     */
    private String toVersion;


    /**
     * 判断是否能升级
     * @param fromVersion 原版本
     * @param toVersion 升级后的版本
     */
    protected VersionPair(String fromVersion, String toVersion) {
        this.fromVersion = fromVersion;
        this.toVersion = toVersion;
    }

    public String getFromVersion() {
        return this.fromVersion;
    }

    public String getToVersion() {
        return this.toVersion;
    }

    @Override
    public String toString() {
        return getFromVersion() + " -> " + getToVersion();
    }
}
