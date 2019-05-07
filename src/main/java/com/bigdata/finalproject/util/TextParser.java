package com.bigdata.finalproject.util;

import com.bigdata.finalproject.topNhelpfulreviews.TopNHelpfulReviewsWritable;
import org.apache.hadoop.io.Text;

public class TextParser {

    public static TopNHelpfulReviewsWritable parseToTopNHelpfulReviewWritable(Text value) {
        String[] values = value.toString().split("\\|---\\|");
        TopNHelpfulReviewsWritable topNHelpfulReviewsWritable = new TopNHelpfulReviewsWritable();
        topNHelpfulReviewsWritable.setMarketPlace(values[0]);
        topNHelpfulReviewsWritable.setCustomerId(values[1]);
        topNHelpfulReviewsWritable.setReviewId(values[2]);
        topNHelpfulReviewsWritable.setProductId(values[3]);
        topNHelpfulReviewsWritable.setProductParent(values[4]);
        topNHelpfulReviewsWritable.setProductTitle(values[5]);
        topNHelpfulReviewsWritable.setProductCategory(values[6]);
        topNHelpfulReviewsWritable.setStarRating(values[7]);
        topNHelpfulReviewsWritable.setHelpfulVotes(values[8]);
        topNHelpfulReviewsWritable.setNotHelpfulVotes(values[9]);
        topNHelpfulReviewsWritable.setTotalVotes(values[10]);
        topNHelpfulReviewsWritable.setActualTotalVotes(values[11]);
        topNHelpfulReviewsWritable.setVine(values[12]);
        topNHelpfulReviewsWritable.setVerifiedPurchase(values[13]);
        topNHelpfulReviewsWritable.setReviewHeadline(values[14]);
        topNHelpfulReviewsWritable.setReviewBody(values[15]);
        topNHelpfulReviewsWritable.setReviewDate(values[16]);
        return topNHelpfulReviewsWritable;

    }

    public static Text tabSeparatedText(String... args) {
        String value = String.join("\t", args);
        return new Text(value);
    }

    public static Text HSeparatedString(String... args) {
        String value = String.join("|---|", args);
        return new Text(value);
    }

    public static Text commaSeparatedString(String... args) {
        String value = String.join(",", args);
        return new Text(value);
    }
}
