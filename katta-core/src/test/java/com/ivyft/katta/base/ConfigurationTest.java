package com.ivyft.katta.base;

import com.ivyft.katta.util.KattaConfiguration;
import com.ivyft.katta.util.PropertyUtil;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.junit.Test;

import java.net.URL;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/6
 * Time: 12:29
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class ConfigurationTest {


    public ConfigurationTest() {
    }


    @Test
    public void testLoadFile() throws Exception {
        KattaConfiguration conf = new KattaConfiguration("katta.node.properties");
        System.out.println(conf.getKeys().hasNext());
        System.out.println(conf.getString("node.solrhome.folder"));
    }


    @Test
    public void testOrigin() throws Exception {
        String path = "katta.node.properties";
        URL url = PropertyUtil.getClasspathResource(path);
        System.out.println(url);

        PropertiesConfiguration conf = new PropertiesConfiguration(url);

        System.out.println(conf.getKeys().next());

    }
}
