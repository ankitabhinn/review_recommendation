package com.bigdata.finalproject;

import com.bigdata.finalproject.recommendation.MahoutRecommendation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

public class ReviewsDriver extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new ReviewsDriver(), args);
        System.exit(res);
    }

    @Override
    public int run(String[] args) throws Exception {
        try {

            Properties prop = new Properties();
            try (InputStream input = ReviewsDriver.class.getClassLoader().getResourceAsStream("app.properties")) {
                if (input == null) {
                    System.out.println("Sorry, unable to find config.properties");
                    return 1;
                }
                //load a properties file from class path, inside static method
                prop.load(input);
            } catch (IOException ex) {
                ex.printStackTrace();
            }

            List<String> propNames = new ArrayList<>();
            Enumeration temp = prop.propertyNames();

            while (temp.hasMoreElements()) {
                propNames.add(temp.nextElement().toString());
            }

            int len = args.length;
            for (int i = 0; i < len; i++) {
                prop.setProperty(propNames.get(i), args[i]);
            }

            Configuration configuration = InitConfig.getConfiguration();

            FileSystem fs = FileSystem.get(configuration);

            // Set all the paths required for jobs
            Path inputPath = new Path(prop.getProperty("inputPath"));
            Path filteredPath = new Path(prop.getProperty("filteredPath"));
            Path meanStdPath = new Path(prop.getProperty("meanStdPath"));
            Path standardizationPath = new Path(prop.getProperty("standardizationPath"));
            Path topNHelpfulReviewsPath = new Path(prop.getProperty("topNHelpfulReviewsPath"));
            Path dataProcessedPath = new Path(prop.getProperty("dataProcessedPath"));
            Path recommendationPath = new Path(prop.getProperty("recommendationPath"));
            Path mappedDataPath = new Path(prop.getProperty("mappedDataPath"));
            Path joinPath = new Path(prop.getProperty("joinPath"));

            ExecJobs jobs = new ExecJobs(configuration, fs);

            boolean isMeanStdJobSuccessful = jobs.MeanStd(inputPath, meanStdPath);

            boolean isFilteredJobSuccessful = false;
            if (isMeanStdJobSuccessful) {
                isFilteredJobSuccessful = jobs.DataFiltering(inputPath, filteredPath);
            }

            boolean isStandardizationJobSuccessful = false;
            if (isFilteredJobSuccessful) {
                isStandardizationJobSuccessful = jobs.Standardization(filteredPath, standardizationPath);
            }

            boolean isTopNHelpfulReviewJobSuccessful = false;
            if (isStandardizationJobSuccessful) {
                isTopNHelpfulReviewJobSuccessful = jobs.TopNHelpfulReview(standardizationPath, topNHelpfulReviewsPath);
            }

            boolean isDataReadyForRecommendation = false;
            if (isTopNHelpfulReviewJobSuccessful) {
                isDataReadyForRecommendation = jobs.DataPrep(inputPath, dataProcessedPath);
            }

            boolean isRecommendationReady = false;
            if (isDataReadyForRecommendation) {
                MahoutRecommendation mahoutRecommendation = MahoutRecommendation.getInstance();
                mahoutRecommendation.setInputPath(dataProcessedPath);
                isRecommendationReady = jobs.GenerateRecommendation(dataProcessedPath, recommendationPath);
            }

            boolean isProductIdMapped = false;
            if (isRecommendationReady) {
                isProductIdMapped = jobs.ProductIdMap(inputPath, mappedDataPath);
            }

            boolean isJoinSuccessful = false;
            if (isProductIdMapped) {
                isJoinSuccessful = jobs.JoinIds(recommendationPath, mappedDataPath, joinPath);
            }

            return (isMeanStdJobSuccessful && isStandardizationJobSuccessful && isDataReadyForRecommendation &&
                    isFilteredJobSuccessful && isTopNHelpfulReviewJobSuccessful && isRecommendationReady &&
                    isProductIdMapped && isJoinSuccessful) ? 0 : 1;

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return 0;
    }

}
