package com.ivyft.katta.hive;

import com.ivyft.katta.hadoop.KattaInputFormat;
import com.ivyft.katta.hadoop.KattaInputSplit;
import com.ivyft.katta.hadoop.KattaSpliter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.solr.client.solrj.SolrQuery;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

/**
 * <pre>
 *
 * Created by IntelliJ IDEA.
 * User: zhenqin
 * Date: 15/4/8
 * Time: 14:40
 * To change this template use File | Settings | File Templates.
 *
 * </pre>
 *
 * @author zhenqin
 */
public class KattaMr1InputFormat
        extends HiveInputFormat<Text, MapWritable> {



    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        List<KattaInputSplit> splits = getSplits(job);
        KattaHiveInputSplit[] splitResult = new KattaHiveInputSplit[splits.size()];
        for (int i = 0; i < splits.size(); i++) {
            splitResult[i] = new KattaHiveInputSplit(splits.get(i), new Path(job.get("location")));
        }
        return splitResult;
    }



    @Override
    public RecordReader<Text, MapWritable> getRecordReader(InputSplit split, JobConf job, Reporter reporter) throws IOException {
        KattaHiveInputSplit ks = (KattaHiveInputSplit)split;
        return new KattaMr1RecordReader(ks.getDelegate(), job);

    }


    public List<KattaInputSplit> getSplits(Configuration conf) throws IOException {
        return (List)KattaSpliter.calculateSplits(conf);
    }


    public static class KattaHiveInputSplit extends FileSplit {

        protected InputSplit delegate;

        protected Path path;


        public KattaHiveInputSplit() {
            this(new KattaInputSplit());
        }

        KattaHiveInputSplit(InputSplit delegate) {
            this(delegate, null);
        }

        KattaHiveInputSplit(InputSplit delegate, Path path) {
            super(path, 0L, 0L, (String[]) null);
            this.delegate = delegate;
            this.path = path;
        }

        public InputSplit getDelegate() {
            return this.delegate;
        }

        public long getLength() {
            return 1L;
        }

        public void write(DataOutput out)
                throws IOException {
            super.write(out);
            Text.writeString(out, this.path.toString());
            this.delegate.write(out);
        }

        public void readFields(DataInput in)
                throws IOException {
            super.readFields(in);
            this.path = new Path(Text.readString(in));
            this.delegate.readFields(in);
        }

        public String toString() {
            return this.delegate.toString();
        }

        public Path getPath() {
            return this.path;
        }


    }


    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        KattaInputFormat.setZookeeperServers(conf, "localhost:2181");
        KattaInputFormat.setIndexNames(conf, "userindex");
        KattaInputFormat.setInputKey(conf, "USER_URN");
        KattaInputFormat.setInputQuery(conf, new SolrQuery("*:*"));

        List<KattaInputSplit> splits = new KattaMr1InputFormat().getSplits(conf);
        System.out.println(splits.size());

        for (KattaInputSplit split : splits) {
            System.out.println(split.getHost() + ":" + split.getPort() + "   " + split.getShardName());
        }
    }
}
