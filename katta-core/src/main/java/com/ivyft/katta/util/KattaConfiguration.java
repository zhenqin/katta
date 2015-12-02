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
package com.ivyft.katta.util;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.Serializable;
import java.util.Properties;
import java.util.Set;

/**
 *
 * Katta的一个配置， 该类是Node和Master配置的超类。
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
public class KattaConfiguration implements Serializable {

    /**
     * 序列化
     */
    private static final long serialVersionUID = 1L;


    /**
     * 配置信息
     */
    protected Properties properties;


    /**
     * 源配置路径
     */
    private final String resourcePath;


    /**
     * Log
     */
    private final static Logger LOG = LoggerFactory.getLogger(KattaConfiguration.class);

    public KattaConfiguration(String path) {
        this.properties = PropertyUtil.loadProperties(path);
        this.resourcePath = PropertyUtil.getPropertiesFilePath(path);
    }

    public KattaConfiguration(File file) {
        this.properties = PropertyUtil.loadProperties(file);
        this.resourcePath = file.getAbsolutePath();
    }

    public KattaConfiguration(Properties properties, String filePath) {
        this.properties = properties;
        this.resourcePath = filePath;
    }

    public KattaConfiguration() {
        this.properties = new Properties();
        this.resourcePath = null;
    }

    public String getResourcePath() {
        return this.resourcePath;
    }

    public boolean containsProperty(String key) {
        return this.properties.containsKey(key);
    }

    public String getProperty(String key) {
        String value = this.properties.getProperty(key);
        if (value == null) {
            throw new IllegalStateException("no property with key '" + key + "' found");
        }
        return value;
    }

    public String getProperty(String key, String defaultValue) {
        String value = this.properties.getProperty(key);
        if (value == null) {
            value = defaultValue;
        }
        return value;
    }

    public void setProperty(String key, String value) {
        this.properties.setProperty(key, value);
    }

    protected void setProperty(String key, long value) {
        this.properties.setProperty(key, Long.toString(value));
    }

    public boolean getBoolean(String key, boolean defaultValue) {
        return Boolean.parseBoolean(getProperty(key, Boolean.toString(defaultValue)));
    }

    public int getInt(String key) {
        return Integer.parseInt(getProperty(key));
    }

    public int getInt(String key, int defaultValue) {
        try {
            return Integer.parseInt(getProperty(key));
        } catch (NumberFormatException e) {
            LOG.warn("failed to parse value '" + getProperty(key) + "' for key '" + key + "' - returning default value '"
                    + defaultValue + "'");
            return defaultValue;
        } catch (IllegalStateException e) {
            return defaultValue;
        }
    }

    public float getFloat(String key, float defaultValue) {
        try {
            return Float.parseFloat(getProperty(key));
        } catch (NumberFormatException e) {
            LOG.warn("failed to parse value '" + getProperty(key) + "' for key '" + key + "' - returning default value '"
                    + defaultValue + "'");
            return defaultValue;
        } catch (IllegalStateException e) {
            return defaultValue;
        }
    }



    public double getDouble(String key, double defaultValue) {
        try {
            return Double.parseDouble(getProperty(key));
        } catch (NumberFormatException e) {
            LOG.warn("failed to parse value '" + getProperty(key) + "' for key '" + key + "' - returning default value '"
                    + defaultValue + "'");
            return defaultValue;
        } catch (IllegalStateException e) {
            return defaultValue;
        }
    }

    public File getFile(String key) {
        String property = System.getProperty(key);
        if(StringUtils.isBlank(property)) {
            property = getProperty(key);
        }
        File file = new File(property);
        LOG.info(key + " dir: " + file.getAbsolutePath());
        if(!file.exists()) {
            if(!file.mkdirs()) {
                throw new IllegalArgumentException("can not mkdir: " + file.getAbsolutePath());
            }
        }
        return file;
    }

    public Class<?> getClass(String key) {
        String className = getProperty(key);
        return ClassUtil.forName(className, Object.class);
    }

    public Class<?> getClass(String key, Class<?> defaultValue) {
        String className = getProperty(key, defaultValue.getName());
        return ClassUtil.forName(className, Object.class);
    }

    public Set<String> getKeys() {
        return this.properties.stringPropertyNames();
    }

    public Properties getPropertiesCopy() {
        return new Properties(this.properties);
    }
}
