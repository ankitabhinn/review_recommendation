package com.bigdata.finalproject.recommendation.dataprocessing;

import com.bigdata.finalproject.util.StringUtil;
import com.bigdata.finalproject.util.TextParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductIdMapMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\t");
        String convertedValue = StringUtil.convertStringToInteger(values[3]);
        Text text = TextParser.tabSeparatedText(values[3], convertedValue);
        context.write(NullWritable.get(), text);
    }
}
