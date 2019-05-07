package com.bigdata.finalproject.recommendation;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;

import java.io.IOException;
import java.util.List;

public class MahoutRecommendationReducer extends Reducer<Text, NullWritable, Text, Text> {

    private MahoutRecommendation mahoutRecommendation = MahoutRecommendation.getInstance();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        mahoutRecommendation.prepareModel();
    }

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        Recommender recommender = mahoutRecommendation.getRecommender();
        try {
            List<RecommendedItem> recommendedItemList = recommender.recommend(Long.parseLong(key.toString()), 3);
            for (RecommendedItem recommendedItem : recommendedItemList) {
                context.write(key, new Text(String.valueOf(recommendedItem.getItemID())));
            }
        } catch (TasteException e) {
            e.printStackTrace();
        }
    }
}
