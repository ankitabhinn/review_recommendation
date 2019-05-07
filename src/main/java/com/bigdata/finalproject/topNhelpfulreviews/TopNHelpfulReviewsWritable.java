package com.bigdata.finalproject.topNhelpfulreviews;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TopNHelpfulReviewsWritable implements Writable {

    private String marketPlace;
    private String customerId;
    private String reviewId;
    private String productId;
    private String productParent;
    private String productTitle;
    private String productCategory;
    private String starRating;
    private String helpfulVotes;
    private String notHelpfulVotes;
    private String totalVotes;
    private String actualTotalVotes;
    private String vine;
    private String verifiedPurchase;
    private String reviewHeadline;
    private String reviewBody;
    private String reviewDate;

    public String getMarketPlace() {
        return marketPlace;
    }

    public void setMarketPlace(String marketPlace) {
        this.marketPlace = marketPlace;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getReviewId() {
        return reviewId;
    }

    public void setReviewId(String reviewId) {
        this.reviewId = reviewId;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductParent() {
        return productParent;
    }

    public void setProductParent(String productParent) {
        this.productParent = productParent;
    }

    public String getProductTitle() {
        return productTitle;
    }

    public void setProductTitle(String productTitle) {
        this.productTitle = productTitle;
    }

    public String getProductCategory() {
        return productCategory;
    }

    public void setProductCategory(String productCategory) {
        this.productCategory = productCategory;
    }

    public String getStarRating() {
        return starRating;
    }

    public void setStarRating(String starRating) {
        this.starRating = starRating;
    }

    public String getHelpfulVotes() {
        return helpfulVotes;
    }

    public void setHelpfulVotes(String helpfulVotes) {
        this.helpfulVotes = helpfulVotes;
    }

    public String getNotHelpfulVotes() {
        return notHelpfulVotes;
    }

    public void setNotHelpfulVotes(String notHelpfulVotes) {
        this.notHelpfulVotes = notHelpfulVotes;
    }

    public String getTotalVotes() {
        return totalVotes;
    }

    public void setTotalVotes(String totalVotes) {
        this.totalVotes = totalVotes;
    }

    public String getActualTotalVotes() {
        return actualTotalVotes;
    }

    public void setActualTotalVotes(String actualTotalVotes) {
        this.actualTotalVotes = actualTotalVotes;
    }

    public String getVine() {
        return vine;
    }

    public void setVine(String vine) {
        this.vine = vine;
    }

    public String getVerifiedPurchase() {
        return verifiedPurchase;
    }

    public void setVerifiedPurchase(String verifiedPurchase) {
        this.verifiedPurchase = verifiedPurchase;
    }

    public String getReviewHeadline() {
        return reviewHeadline;
    }

    public void setReviewHeadline(String reviewHeadline) {
        this.reviewHeadline = reviewHeadline;
    }

    public String getReviewBody() {
        return reviewBody;
    }

    public void setReviewBody(String reviewBody) {
        this.reviewBody = reviewBody;
    }

    public String getReviewDate() {
        return reviewDate;
    }

    public void setReviewDate(String reviewDate) {
        this.reviewDate = reviewDate;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, getMarketPlace());
        WritableUtils.writeString(out, getCustomerId());
        WritableUtils.writeString(out, getReviewId());
        WritableUtils.writeString(out, getProductId());
        WritableUtils.writeString(out, getProductParent());
        WritableUtils.writeString(out, getProductTitle());
        WritableUtils.writeString(out, getProductCategory());
        WritableUtils.writeString(out, getStarRating());
        WritableUtils.writeString(out, getHelpfulVotes());
        WritableUtils.writeString(out, getNotHelpfulVotes());
        WritableUtils.writeString(out, getTotalVotes());
        WritableUtils.writeString(out, getActualTotalVotes());
        WritableUtils.writeString(out, getVine());
        WritableUtils.writeString(out, getVerifiedPurchase());
        WritableUtils.writeString(out, getReviewHeadline());
        WritableUtils.writeString(out, getReviewBody());
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.setMarketPlace(WritableUtils.readString(in));
        this.setCustomerId(WritableUtils.readString(in));
        this.setReviewId(WritableUtils.readString(in));
        this.setProductId(WritableUtils.readString(in));
        this.setProductParent(WritableUtils.readString(in));
        this.setProductTitle(WritableUtils.readString(in));
        this.setProductCategory(WritableUtils.readString(in));
        this.setStarRating(WritableUtils.readString(in));
        this.setHelpfulVotes(WritableUtils.readString(in));
        this.setNotHelpfulVotes(WritableUtils.readString(in));
        this.setTotalVotes(WritableUtils.readString(in));
        this.setActualTotalVotes(WritableUtils.readString(in));
        this.setVine(WritableUtils.readString(in));
        this.setVerifiedPurchase(WritableUtils.readString(in));
        this.setReviewHeadline(WritableUtils.readString(in));
        this.setReviewBody(WritableUtils.readString(in));
        this.setReviewDate(WritableUtils.readString(in));
    }

    @Override
    public String toString() {
        return "TopNHelpfulReviewsWritable{" +
                "marketPlace='" + marketPlace + '\'' +
                ", customerId='" + customerId + '\'' +
                ", reviewId='" + reviewId + '\'' +
                ", productId='" + productId + '\'' +
                ", productParent='" + productParent + '\'' +
                ", productTitle='" + productTitle + '\'' +
                ", productCategory='" + productCategory + '\'' +
                ", starRating='" + starRating + '\'' +
                ", helpfulVotes='" + helpfulVotes + '\'' +
                ", notHelpfulVotes='" + notHelpfulVotes + '\'' +
                ", totalVotes='" + totalVotes + '\'' +
                ", vine='" + vine + '\'' +
                ", verifiedPurchase='" + verifiedPurchase + '\'' +
                ", reviewHeadline='" + reviewHeadline + '\'' +
                ", reviewBody='" + reviewBody + '\'' +
                ", reviewDate='" + reviewDate + '\'' +
                '}';
    }
}
