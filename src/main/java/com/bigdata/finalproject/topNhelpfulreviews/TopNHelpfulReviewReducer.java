package com.bigdata.finalproject.topNhelpfulreviews;

import com.bigdata.finalproject.util.TextParser;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.SortedMap;
import java.util.TreeMap;

public class TopNHelpfulReviewReducer extends Reducer<Text, Text, Text, Text> {

    @SuppressWarnings("Duplicates")
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        SortedMap<TopNHelpfulReviewsWritable, TopNHelpfulReviewsWritable> sortedMap = new TreeMap<>(new TreeComparator());
        for (Text value : values) {
            TopNHelpfulReviewsWritable topNHelpfulReviewsWritable = TextParser.parseToTopNHelpfulReviewWritable(value);

            if (topNHelpfulReviewsWritable == null)
                continue;

            if (sortedMap.isEmpty()) {
                sortedMap.put(topNHelpfulReviewsWritable, topNHelpfulReviewsWritable);
            }

            boolean shouldAdd = false;
            for (TopNHelpfulReviewsWritable k : sortedMap.keySet())
                if (!k.toString().equalsIgnoreCase(topNHelpfulReviewsWritable.toString()))
                    shouldAdd = true;

            if (shouldAdd)
                sortedMap.put(topNHelpfulReviewsWritable, topNHelpfulReviewsWritable);

            if (sortedMap.size() > 3) {
                sortedMap.remove(sortedMap.firstKey());
            }
        }

        for (TopNHelpfulReviewsWritable t : sortedMap.values()) {
            Text value = TextParser.tabSeparatedText(t.getMarketPlace(), t.getCustomerId(), t.getReviewId(),
                    t.getProductId(), t.getProductParent(), t.getProductTitle(), t.getProductCategory(),
                    t.getStarRating(), t.getHelpfulVotes(), t.getNotHelpfulVotes(), t.getTotalVotes(), t.getActualTotalVotes(),
                    t.getVine(), t.getVerifiedPurchase(), t.getReviewHeadline(), t.getReviewBody(), t.getReviewDate());
            context.write(key, value);
        }

    }
}
