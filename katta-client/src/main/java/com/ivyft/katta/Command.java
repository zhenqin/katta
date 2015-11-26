package com.ivyft.katta;

import com.ivyft.katta.util.ZkConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;


/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 14-3-22
 * Time: 下午12:23
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public abstract class Command {


    /**
     * run command
     */
    protected String command;


    /**
     * description
     */
    protected String header;


    /**
     *
     * @param command command
     * @param header description
     */
    protected Command(String command, String header) {
        this.command = command;
        this.header = header;
    }



    public String getCommand() {
        return command;
    }


    /**
     * @return the options this client will process.
     */
    public abstract Options getOpts();


    /**
     * @return header description for this command
     */
    public String getHeaderDescription() {
        return header;
    }

    /**
     * Do the processing
     * @param cl the arguments to process
     * @throws Exception on any error
     */
    public abstract void process(CommandLine cl) throws Exception;




    protected abstract void execute(ZkConfiguration zkConf) throws Exception;

}
