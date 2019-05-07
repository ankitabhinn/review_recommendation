package com.bigdata.finalproject.recommendation.dataprocessing;

import com.bigdata.finalproject.util.StringUtil;
import com.bigdata.finalproject.util.TextParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductIdConverterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\t");
        if (values[0].equalsIgnoreCase("marketplace"))
            return;
        if (values[10].equalsIgnoreCase("y")) {
            String longValue = StringUtil.convertStringToInteger(values[3]);
            Text text = TextParser.commaSeparatedString(values[1], longValue, values[7]);
            context.write(NullWritable.get(), text);
        }
    }
}
