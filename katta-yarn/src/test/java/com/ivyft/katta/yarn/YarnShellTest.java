package com.ivyft.katta.yarn;

import org.apache.hadoop.util.Shell;
import org.junit.Test;

import java.util.Arrays;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/14
 * Time: 10:23
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class YarnShellTest {


    @Test
    public void testPrintKillShell() throws Exception {
        System.out.println(Arrays.toString(Shell.getSignalKillCommand(15, "2760")));

    }
}
