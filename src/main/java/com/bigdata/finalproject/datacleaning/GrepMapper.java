package com.bigdata.finalproject.datacleaning;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class GrepMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private String mapRegex = null;

    @Override
    protected void setup(Context context) {
        mapRegex = context.getConfiguration().get("mapregex");
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        if (!value.toString().matches(mapRegex)) {
            context.write(NullWritable.get(), value);
        }
    }
}
