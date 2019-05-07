package com.bigdata.finalproject.summarization;

import com.bigdata.finalproject.util.TextParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MeanStdReducer extends Reducer<Text, MeanStdWritable, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<MeanStdWritable> values, Context context) throws IOException, InterruptedException {
        double helpfulCount = 0;
        double helpfulSum = 0;
        double helpfulMean = 0;
        double helpfulStd = 0;
        double totalCount = 0;
        double totalSum = 0;
        double totalMean = 0;
        double totalStd = 0;
        double notHelpfulCount = 0;
        double notHelpfulSum = 0;
        double notHelpfulMean = 0;
        double notHelpfulStd = 0;
        List<MeanStdWritable> cache = new ArrayList<>();
        for (MeanStdWritable v : values) {
            notHelpfulCount += Integer.parseInt(v.getNotHelpfulCount());
            notHelpfulSum += Integer.parseInt(v.getNotHelpfulSum());
            helpfulCount += Integer.parseInt(v.getHelpfulCount());
            helpfulSum += Integer.parseInt(v.getHelpfulSum());
            totalCount += Integer.parseInt(v.getTotalCount());
            totalSum += Integer.parseInt(v.getTotalSum());
            cache.add(v);
        }

        double hstdnumerator = 0;
        double tstdnumerator = 0;
        double nhstdnumerator = 0;

        helpfulMean = helpfulSum / helpfulCount;
        totalMean = totalSum / totalCount;
        notHelpfulMean = notHelpfulSum / notHelpfulCount;

        for (MeanStdWritable v : cache) {
            nhstdnumerator += Math.pow(Double.parseDouble(v.getNotHelpfulSum()) - notHelpfulMean, 2);
            hstdnumerator += Math.pow(Double.parseDouble(v.getHelpfulSum()) - helpfulMean, 2);
            tstdnumerator += Math.pow(Double.parseDouble(v.getTotalSum()) - totalMean, 2);
        }
        helpfulStd = Math.sqrt(hstdnumerator / helpfulCount);
        notHelpfulStd = Math.sqrt(nhstdnumerator / notHelpfulCount);
        totalStd = Math.sqrt(tstdnumerator / totalCount);

        Text newValues = TextParser.tabSeparatedText(String.valueOf(helpfulMean), String.valueOf(helpfulStd),
                String.valueOf(notHelpfulMean), String.valueOf(notHelpfulStd),
                String.valueOf(totalMean), String.valueOf(totalStd));
        context.write(key, newValues);
    }

}
