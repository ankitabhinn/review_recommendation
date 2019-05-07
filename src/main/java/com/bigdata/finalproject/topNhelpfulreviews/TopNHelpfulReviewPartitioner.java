package com.bigdata.finalproject.topNhelpfulreviews;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

public class TopNHelpfulReviewPartitioner extends Partitioner<IntWritable, Text> implements Configurable {

    private static final String MIN_ID = "min.id";
    private Configuration conf = null;
    private int minId = 0;

    public static void setMinId(Job job, int minId) {
        job.getConfiguration().setInt(MIN_ID, minId);
    }

    @Override
    public int getPartition(IntWritable key, Text value, int i) {
        return key.get() - minId;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration c) {
        this.conf = c;
        minId = conf.getInt(MIN_ID, 0);
    }

}
