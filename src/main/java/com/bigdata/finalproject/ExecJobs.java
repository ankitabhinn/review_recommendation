package com.bigdata.finalproject;

import com.bigdata.finalproject.datacleaning.GrepMapper;
import com.bigdata.finalproject.join.ProductIdJoinMapper;
import com.bigdata.finalproject.join.ProductIdReducer;
import com.bigdata.finalproject.join.RecommendationJoinMapper;
import com.bigdata.finalproject.recommendation.MahoutRecommendationMapper;
import com.bigdata.finalproject.recommendation.MahoutRecommendationReducer;
import com.bigdata.finalproject.recommendation.dataprocessing.ProductIdConverterMapper;
import com.bigdata.finalproject.recommendation.dataprocessing.ProductIdMapMapper;
import com.bigdata.finalproject.standardization.StandardizationMapper;
import com.bigdata.finalproject.standardization.StandardizationReducer;
import com.bigdata.finalproject.summarization.MeanStdMapper;
import com.bigdata.finalproject.summarization.MeanStdReducer;
import com.bigdata.finalproject.summarization.MeanStdWritable;
import com.bigdata.finalproject.topNhelpfulreviews.TopNHelpfulReviewCombiner;
import com.bigdata.finalproject.topNhelpfulreviews.TopNHelpfulReviewMapper;
import com.bigdata.finalproject.topNhelpfulreviews.TopNHelpfulReviewPartitioner;
import com.bigdata.finalproject.topNhelpfulreviews.TopNHelpfulReviewReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

class ExecJobs {

    private final Configuration configuration;
    private final FileSystem fs;

    ExecJobs(Configuration configuration, FileSystem fs) {
        this.configuration = configuration;
        this.fs = fs;
    }

    boolean MeanStd(Path inputPath, Path meanStdPath) throws IOException, ClassNotFoundException, InterruptedException {

        // Define MapReduce Job
        Job meanStdJob = Job.getInstance(configuration, "DataFiltering Helpful Reviews");
        meanStdJob.setJarByClass(ReviewsDriver.class);

        // Set Input and Output locations for MeanStdHelpfulReviews Job
        FileInputFormat.setInputPaths(meanStdJob, inputPath);
        FileOutputFormat.setOutputPath(meanStdJob, meanStdPath);

        // Set Input and Output formats for MeanStdHelpfulReviews Job
        meanStdJob.setInputFormatClass(TextInputFormat.class);
        meanStdJob.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper/Combiner/Reducer classes for MeanStdHelpfulReviews Job
        meanStdJob.setMapperClass(MeanStdMapper.class);
        meanStdJob.setReducerClass(MeanStdReducer.class);

        // Set key/values classes
        meanStdJob.setMapOutputKeyClass(Text.class);
        meanStdJob.setMapOutputValueClass(MeanStdWritable.class);
        meanStdJob.setOutputKeyClass(NullWritable.class);
        meanStdJob.setOutputValueClass(MeanStdReducer.class);

        // Check if the output path is available or not
        if (fs.exists(meanStdPath)) {
            fs.delete(meanStdPath, true);
        }

        return meanStdJob.waitForCompletion(true);
    }


    boolean DataFiltering(Path inputPath, Path filteredPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Define MapReduce Job
        Job job = Job.getInstance(configuration, "Data Cleaning");
        job.setJarByClass(ReviewsDriver.class);
        job.getConfiguration().set("mapregex", "(.*)\\t[1-5]{1}\\t[0]{1}\\t[0]{1}\\t(.*)");

        // Set Input and Output locations for DataCleaning job
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, filteredPath);

        // Set Input and Output formats for DataCleaning job
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper classes for DataCleaning job
        job.setMapperClass(GrepMapper.class);

        // Specify map output types for DataCleaning job
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        // Check if the output path is available or not
        if (fs.exists(filteredPath)) {
            fs.delete(filteredPath, true);
        }

        return job.waitForCompletion(true);
    }


    boolean Standardization(Path filteredPath, Path standardizationPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Define MapReduce Job
        Job standardizationJob = Job.getInstance(configuration, "Standardization");
        standardizationJob.setJarByClass(ReviewsDriver.class);

        // Set Input and Output locations for Feature Scaling job
        FileInputFormat.setInputPaths(standardizationJob, filteredPath);
        FileOutputFormat.setOutputPath(standardizationJob, standardizationPath);

        // Set Input and Output formats for Feature Scaling job
        standardizationJob.setInputFormatClass(TextInputFormat.class);
        standardizationJob.setOutputFormatClass(TextOutputFormat.class);

        standardizationJob.setNumReduceTasks(1);

        // Set Mapper classes for Feature Scaling job
        standardizationJob.setMapperClass(StandardizationMapper.class);
        standardizationJob.setReducerClass(StandardizationReducer.class);

        // Specify output types for Feature Scaling job
        standardizationJob.setMapOutputKeyClass(Text.class);
        standardizationJob.setMapOutputValueClass(Text.class);
        standardizationJob.setOutputKeyClass(NullWritable.class);
        standardizationJob.setOutputValueClass(Text.class);

        // Check if the output path is available or not
        if (fs.exists(standardizationPath)) {
            fs.delete(standardizationPath, true);
        }

        return standardizationJob.waitForCompletion(true);
    }


    boolean TopNHelpfulReview(Path standardizationPath, Path topNHelpfulReviewsPath)
            throws InterruptedException, IOException, ClassNotFoundException {

        // Define Job
        Job topNHelpfulReviewsJob = Job.getInstance(configuration, "Top N Helpful Reviews");
        topNHelpfulReviewsJob.setJarByClass(ReviewsDriver.class);

        // Set Input and Output locations for Top10Helpful Review job
        FileInputFormat.setInputPaths(topNHelpfulReviewsJob, standardizationPath);
        FileOutputFormat.setOutputPath(topNHelpfulReviewsJob, topNHelpfulReviewsPath);

        // Set Input and Output formats for Top10Helpful Review job
        topNHelpfulReviewsJob.setInputFormatClass(TextInputFormat.class);
        topNHelpfulReviewsJob.setOutputFormatClass(TextOutputFormat.class);

        // Set Partitioner class for Top10Helpful Review job
        TopNHelpfulReviewPartitioner.setMinId(topNHelpfulReviewsJob, 1);
        topNHelpfulReviewsJob.setPartitionerClass(TopNHelpfulReviewPartitioner.class);

        // Set Mapper classes for Top10Helpful Review job
        topNHelpfulReviewsJob.setMapperClass(TopNHelpfulReviewMapper.class);
        topNHelpfulReviewsJob.setCombinerClass(TopNHelpfulReviewCombiner.class);
        topNHelpfulReviewsJob.setReducerClass(TopNHelpfulReviewReducer.class);

        topNHelpfulReviewsJob.setNumReduceTasks(1);

        // Specify map output types for Top10Helpful Review job
        topNHelpfulReviewsJob.setMapOutputKeyClass(Text.class);
        topNHelpfulReviewsJob.setMapOutputValueClass(Text.class);
        topNHelpfulReviewsJob.setOutputKeyClass(Text.class);
        topNHelpfulReviewsJob.setOutputValueClass(Text.class);

        // Check if the output path is available or not
        if (fs.exists(topNHelpfulReviewsPath)) {
            fs.delete(topNHelpfulReviewsPath, true);
        }

        return topNHelpfulReviewsJob.waitForCompletion(true);
    }


    boolean DataPrep(Path inputPath, Path dataProcessedPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Define Job
        Job dataPrepJob = Job.getInstance(configuration, "Data preparation");
        dataPrepJob.setJarByClass(ReviewsDriver.class);

        // Set Input and Output locations
        FileInputFormat.setInputPaths(dataPrepJob, inputPath);
        FileOutputFormat.setOutputPath(dataPrepJob, dataProcessedPath);

        // Set Input and Output formats
        dataPrepJob.setInputFormatClass(TextInputFormat.class);
        dataPrepJob.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper classes for DataCleaning job
        dataPrepJob.setMapperClass(ProductIdConverterMapper.class);

        // Specify map output types for DataCleaning job
        dataPrepJob.setMapOutputKeyClass(NullWritable.class);
        dataPrepJob.setMapOutputValueClass(Text.class);

        // Check if the output path is available or not
        if (fs.exists(dataProcessedPath)) {
            fs.delete(dataProcessedPath, true);
        }

        return dataPrepJob.waitForCompletion(true);
    }


    boolean GenerateRecommendation(Path dataProcessedPath, Path recommendationPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Define Job
        Job recommendationJob = Job.getInstance(configuration, "Recommendation");
        recommendationJob.setJarByClass(ReviewsDriver.class);

        // Set Input and Output locations
        FileInputFormat.setInputPaths(recommendationJob, dataProcessedPath);
        FileOutputFormat.setOutputPath(recommendationJob, recommendationPath);

        // Set Input and Output formats
        recommendationJob.setInputFormatClass(TextInputFormat.class);
        recommendationJob.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper classes for DataCleaning job
        recommendationJob.setMapperClass(MahoutRecommendationMapper.class);
        recommendationJob.setReducerClass(MahoutRecommendationReducer.class);

        // Specify map output types for DataCleaning job
        recommendationJob.setMapOutputKeyClass(Text.class);
        recommendationJob.setMapOutputValueClass(NullWritable.class);

        // Check if the output path is available or not
        if (fs.exists(recommendationPath)) {
            fs.delete(recommendationPath, true);
        }

        return recommendationJob.waitForCompletion(true);
    }


    boolean ProductIdMap(Path inputPath, Path mappedDataPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Define Job
        Job dataPrepJob = Job.getInstance(configuration, "Product ID mapper");
        dataPrepJob.setJarByClass(ReviewsDriver.class);

        // Set Input and Output locations
        FileInputFormat.setInputPaths(dataPrepJob, inputPath);
        FileOutputFormat.setOutputPath(dataPrepJob, mappedDataPath);

        // Set Input and Output formats
        dataPrepJob.setInputFormatClass(TextInputFormat.class);
        dataPrepJob.setOutputFormatClass(TextOutputFormat.class);

        // Set Mapper classes for DataCleaning job
        dataPrepJob.setMapperClass(ProductIdMapMapper.class);

        // Specify map output types for DataCleaning job
        dataPrepJob.setMapOutputKeyClass(NullWritable.class);
        dataPrepJob.setMapOutputValueClass(Text.class);

        // Check if the output path is available or not
        if (fs.exists(mappedDataPath)) {
            fs.delete(mappedDataPath, true);
        }

        return dataPrepJob.waitForCompletion(true);
    }


    boolean JoinIds(Path recommendationPath, Path mappedDataPath, Path joinPath)
            throws IOException, ClassNotFoundException, InterruptedException {

        // Define Job
        Job joinJob = Job.getInstance(configuration, "Join with ids");
        joinJob.setJarByClass(ReviewsDriver.class);


        // Set Mappers and Reducers
        MultipleInputs.addInputPath(joinJob, recommendationPath, KeyValueTextInputFormat.class, RecommendationJoinMapper.class);
        MultipleInputs.addInputPath(joinJob, mappedDataPath, KeyValueTextInputFormat.class, ProductIdJoinMapper.class);
        joinJob.setReducerClass(ProductIdReducer.class);

        // Specify output types
        joinJob.setMapOutputKeyClass(Text.class);
        joinJob.setMapOutputValueClass(Text.class);
        joinJob.setOutputKeyClass(Text.class);
        joinJob.setOutputValueClass(Text.class);

        // Set output format
        joinJob.setOutputFormatClass(TextOutputFormat.class);

        // Check if the output path is available or not
        if (fs.exists(joinPath)) {
            fs.delete(joinPath, true);
        }

        TextOutputFormat.setOutputPath(joinJob, joinPath);

        return joinJob.waitForCompletion(true);

    }

}
