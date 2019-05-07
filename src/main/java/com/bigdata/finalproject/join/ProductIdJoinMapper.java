package com.bigdata.finalproject.join;

import com.bigdata.finalproject.util.TextParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ProductIdJoinMapper extends Mapper<Text, Text, Text, Text> {

    private Text emitKey = new Text();
    private Text emitValue = new Text();

    @Override
    protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
        emitKey.set(value);
        emitValue.set("!" + TextParser.tabSeparatedText(key.toString(), value.toString()));
        context.write(emitKey, emitValue);
    }

}
