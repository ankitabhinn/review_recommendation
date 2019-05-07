package com.bigdata.finalproject.topNhelpfulreviews;

import java.util.Comparator;

public class TreeComparator implements Comparator<TopNHelpfulReviewsWritable> {

    @Override
    public int compare(TopNHelpfulReviewsWritable o1, TopNHelpfulReviewsWritable o2) {

        double score1 = Double.parseDouble(o1.getHelpfulVotes()) -
                Double.parseDouble(o1.getNotHelpfulVotes()) + Double.parseDouble(o1.getTotalVotes());
        double score2 = Double.parseDouble(o2.getHelpfulVotes()) -
                Double.parseDouble(o2.getNotHelpfulVotes()) + Double.parseDouble(o2.getTotalVotes());
        return Double.compare(score2, score1);

    }
}
