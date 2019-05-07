package com.bigdata.finalproject.summarization.model;

import java.util.HashMap;
import java.util.Map;

public class MeanStdModelWrapper {

    public Map<String, MeanStdModel> meanStdModelMap = new HashMap<>();

    public static class MeanStdModel {

        double helpfulReviewMean = 0;
        double helpfulReviewStd = 0;
        double notHelpfulReviewMean = 0;
        double notHelpfulReviewStd = 0;
        double totalVoteMean = 0;
        double totalVoteStd = 0;

        public double getHelpfulReviewMean() {
            return helpfulReviewMean;
        }

        public void setHelpfulReviewMean(double helpfulReviewMean) {
            this.helpfulReviewMean = helpfulReviewMean;
        }

        public double getHelpfulReviewStd() {
            return helpfulReviewStd;
        }

        public void setHelpfulReviewStd(double helpfulReviewStd) {
            this.helpfulReviewStd = helpfulReviewStd;
        }

        public double getNotHelpfulReviewMean() {
            return notHelpfulReviewMean;
        }

        public void setNotHelpfulReviewMean(double notHelpfulReviewMean) {
            this.notHelpfulReviewMean = notHelpfulReviewMean;
        }

        public double getNotHelpfulReviewStd() {
            return notHelpfulReviewStd;
        }

        public void setNotHelpfulReviewStd(double notHelpfulReviewStd) {
            this.notHelpfulReviewStd = notHelpfulReviewStd;
        }

        public double getTotalVoteMean() {
            return totalVoteMean;
        }

        public void setTotalVoteMean(double totalVoteMean) {
            this.totalVoteMean = totalVoteMean;
        }

        public double getTotalVoteStd() {
            return totalVoteStd;
        }

        public void setTotalVoteStd(double totalVoteStd) {
            this.totalVoteStd = totalVoteStd;
        }
    }

}
