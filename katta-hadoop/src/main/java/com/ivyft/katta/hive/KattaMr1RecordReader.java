package com.ivyft.katta.hive;

import com.ivyft.katta.hadoop.KattaInputSplit;
import com.ivyft.katta.hadoop.KattaSocketReader;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.solr.common.SolrDocument;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/4/8
 * Time: 14:44
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaMr1RecordReader implements RecordReader<Text, MapWritable> {
    protected KattaSocketReader in;

    protected final AtomicLong counter = new AtomicLong(0);



    protected static Log LOG = LogFactory.getLog(KattaMr1RecordReader.class);



    public KattaMr1RecordReader(InputSplit split, JobConf job) throws IOException {
        this.in = new KattaSocketReader((KattaInputSplit) split);
        this.in.initialize((KattaInputSplit) split);
    }



    /**
     * Return the progress within the input split
     * @return 0.0 to 1.0 of the input byte range
     */
    @Override
    public float getProgress() throws IOException {
        return in.getProgress();
    }


    @Override
    public boolean next(Text key, MapWritable value) throws IOException {
        this.counter.addAndGet(1);

        String str = null;
        SolrDocument document = null;
        if(in.nextKeyValue()) {
            str = String.valueOf(in.getCurrentKey());
            document = in.getCurrentValue();
        }

        if(document != null) {
            key.set(str);

            for (Map.Entry<String, Object> entry : document) {
                value.put(new Text(entry.getKey().toUpperCase()), resorve(entry.getValue()));
            }
        }

        if(counter.get() % 1000 == 0) {
            LOG.info("process counter: " + counter.get());
        }

        return document != null;
    }

    private Writable resorve(Object value) {
        if(value == null) {
            return NullWritable.get();
        } else if(value instanceof String) {
            return new Text((String)value);
        } else if(value instanceof Integer) {
            return new IntWritable((Integer) value);
        } else if(value instanceof Long) {
            return new LongWritable((Long)value);
        } else if(value instanceof Float) {
            return new FloatWritable((Float)value);
        } else if(value instanceof Double) {
            return new DoubleWritable((Double)value);
        } else if(value instanceof Collection) {
            Collection cs = (Collection)value;
            if(cs.isEmpty()) {
                return new ArrayWritable(new String[]{});
            }
            Writable[] vs = new Writable[cs.size()];
            int i = 0;
            for (Object c : cs) {
                vs[i] = resorve(c);
                i++;
            }
            return new ArrayWritable(vs[0].getClass(), vs);
        } else if(value instanceof Short) {
            return new ShortWritable((Short)value);
        } else if(value instanceof Boolean) {
            return new BooleanWritable((Boolean)value);
        }
        return new Text(String.valueOf(value));
    }

    @Override
    public Text createKey() {
        Text key = new Text();
        return key;
    }

    @Override
    public MapWritable createValue() {
        MapWritable v = new MapWritable();
        return v;
    }

    @Override
    public long getPos() throws IOException {
        return counter.get();
    }

    @Override
    public synchronized void close() throws IOException {
        in.close();
    }

}
