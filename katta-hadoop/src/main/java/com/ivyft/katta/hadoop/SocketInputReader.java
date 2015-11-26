package com.ivyft.katta.hadoop;


import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;


/**
 * <pre>
 * <p>
 * </p>
 * Created by IntelliJ IDEA.
 * User: ZhenQin
 * Date: 13-9-29
 * Time: 下午3:39
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author ZhenQin
 */
public class SocketInputReader extends RecordReader<Object, SolrDocument> {



	private KattaInputSplit split;



    private KattaSocketReader kattaSocketReader;


	/**
	 * 构造方法
	 *
	 * @param split
	 */
	public SocketInputReader(KattaInputSplit split) {
		this.split = split;
        kattaSocketReader = new KattaSocketReader(split);
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
        kattaSocketReader.initialize(split);
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
        return kattaSocketReader.nextKeyValue();
	}

	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		return kattaSocketReader.getCurrentKey();
	}

	@Override
	public SolrDocument getCurrentValue() throws IOException,
			InterruptedException {
		return kattaSocketReader.getCurrentValue();
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return kattaSocketReader.getProgress();
	}


	/**
	 * Close the record reader.
	 */
	@Override
    public void close() throws IOException {
        kattaSocketReader.close();
    }

}
