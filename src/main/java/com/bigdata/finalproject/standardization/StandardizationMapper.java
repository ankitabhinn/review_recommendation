package com.bigdata.finalproject.standardization;

import com.bigdata.finalproject.util.TextParser;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class StandardizationMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] values = value.toString().split("\\t");
        if (values[0].equalsIgnoreCase("marketplace"))
            return;
        int notHelpfulVote = Integer.parseInt(values[9]) - Integer.parseInt(values[8]);
        Text text = TextParser.tabSeparatedText(values[0], values[1], values[2], values[3],
                values[4], values[5], values[6], values[7], values[8],
                String.valueOf(notHelpfulVote), values[9], values[10], values[11],
                values[12], values[13], values[14]);
        context.write(new Text(values[3]), text);
    }
}
