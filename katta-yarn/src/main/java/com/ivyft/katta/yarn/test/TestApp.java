package com.ivyft.katta.yarn.test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/12/1
 * Time: 09:40
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class TestApp implements Runnable {


    Thread thread = new Thread(this);


    Logger LOG = LoggerFactory.getLogger(TestApp.class);

    public TestApp() {


    }


    @Override
    public void run() {
        try {
            while (true) {
                LOG.info("================"+new Date()+"==================");
                Thread.sleep(3000);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        TestApp app = new TestApp();
        app.thread.setName("test-thread");
        app.thread.start();
    }

}
