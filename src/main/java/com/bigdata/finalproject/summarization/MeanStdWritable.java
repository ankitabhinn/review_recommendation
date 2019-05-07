package com.bigdata.finalproject.summarization;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class MeanStdWritable implements Writable {

    private String productId;
    private String helpfulCount;
    private String helpfulSum;
    private String totalCount;
    private String totalSum;
    private String notHelpfulCount;
    private String notHelpfulSum;

    protected String getProductId() {
        return productId;
    }

    protected void setProductId(String productId) {
        this.productId = productId;
    }

    protected String getHelpfulCount() {
        return helpfulCount;
    }

    protected void setHelpfulCount(String helpfulCount) {
        this.helpfulCount = helpfulCount;
    }

    protected String getHelpfulSum() {
        return helpfulSum;
    }

    protected void setHelpfulSum(String helpfulSum) {
        this.helpfulSum = helpfulSum;
    }

    protected String getTotalCount() {
        return totalCount;
    }

    protected void setTotalCount(String totalCount) {
        this.totalCount = totalCount;
    }

    protected String getTotalSum() {
        return totalSum;
    }

    protected void setTotalSum(String totalSum) {
        this.totalSum = totalSum;
    }

    protected String getNotHelpfulCount() {
        return notHelpfulCount;
    }

    protected void setNotHelpfulCount(String notHelpfulCount) {
        this.notHelpfulCount = notHelpfulCount;
    }

    protected String getNotHelpfulSum() {
        return notHelpfulSum;
    }

    protected void setNotHelpfulSum(String notHelpfulSum) {
        this.notHelpfulSum = notHelpfulSum;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, productId);
        WritableUtils.writeString(out, helpfulCount);
        WritableUtils.writeString(out, helpfulSum);
        WritableUtils.writeString(out, totalCount);
        WritableUtils.writeString(out, totalSum);
        WritableUtils.writeString(out, notHelpfulCount);
        WritableUtils.writeString(out, notHelpfulSum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        setProductId(WritableUtils.readString(in));
        setHelpfulCount(WritableUtils.readString(in));
        setHelpfulSum(WritableUtils.readString(in));
        setTotalCount(WritableUtils.readString(in));
        setTotalSum(WritableUtils.readString(in));
        setNotHelpfulCount(WritableUtils.readString(in));
        setNotHelpfulSum(WritableUtils.readString(in));
    }

    @Override
    public String toString() {
        return productId + '\t' + helpfulCount + '\t' + helpfulSum + '\t' + totalCount + '\t' +
                totalSum + '\t' + notHelpfulCount + '\t' + notHelpfulSum + '\t';
    }
}
