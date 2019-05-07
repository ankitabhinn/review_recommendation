package com.bigdata.finalproject.summarization;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MeanStdMapper extends Mapper<LongWritable, Text, Text, MeanStdWritable> {

    private MeanStdWritable meanStdWritable = new MeanStdWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] values = value.toString().split("\\t");
        if (values[0].equalsIgnoreCase("marketplace"))
            return;
        meanStdWritable.setProductId(values[3]);
        meanStdWritable.setHelpfulSum(values[8]);
        meanStdWritable.setHelpfulCount("1");
        meanStdWritable.setTotalSum(values[9]);
        meanStdWritable.setTotalCount("1");
        int notHelpful = Integer.parseInt(values[9]) - Integer.parseInt(values[8]);
        meanStdWritable.setNotHelpfulSum(String.valueOf(notHelpful));
        meanStdWritable.setNotHelpfulCount("1");
        context.write(new Text(values[3]), meanStdWritable);
    }

}
