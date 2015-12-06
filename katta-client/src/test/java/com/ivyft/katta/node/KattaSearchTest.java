package com.ivyft.katta.node;

import com.ivyft.katta.Katta;
import org.junit.Test;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/2
 * Time: 17:35
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaSearchTest {

    @Test
    public void testSearch() throws Exception {
        Katta.main(new String[]{"search","-s", "test", "*:*"});

    }
}
