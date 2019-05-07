package com.bigdata.finalproject.standardization;

import com.bigdata.finalproject.ReviewsDriver;
import com.bigdata.finalproject.summarization.model.MeanStdModelWrapper;
import com.bigdata.finalproject.util.TextParser;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;

public class StandardizationReducer extends Reducer<Text, Text, NullWritable, Text> {

    private MeanStdModelWrapper meanStdModelWrapper = new MeanStdModelWrapper();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Properties prop = new Properties();
        try (InputStream input = ReviewsDriver.class.getClassLoader().getResourceAsStream("app.properties")) {
            if (input == null) {
                return;
            }
            //load a properties file from class path, inside static method
            prop.load(input);
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        Path meanStdPath = new Path(prop.getProperty("meanStdPath"));

        FileSystem hdfs = FileSystem.get(context.getConfiguration());

        FileStatus[] inputFiles = hdfs.listStatus(meanStdPath);

        StringBuilder finalBuilder = new StringBuilder();
        Arrays.stream(inputFiles).forEach(inputFile -> {
            try (FSDataInputStream hdfsIn = hdfs.open(inputFile.getPath())) {
                finalBuilder.append(IOUtils.toString(hdfsIn, "UTF-8"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        String[] lines = finalBuilder.toString().split("\\n");
        Arrays.stream(lines).forEach(line -> {
            String[] value = line.split("\\t");
            if (value.length > 1) {
                MeanStdModelWrapper.MeanStdModel model = new MeanStdModelWrapper.MeanStdModel();
                model.setHelpfulReviewMean(Double.parseDouble(value[1]));
                model.setHelpfulReviewStd(Double.parseDouble(value[2]));
                model.setNotHelpfulReviewMean(Double.parseDouble(value[3]));
                model.setNotHelpfulReviewStd(Double.parseDouble(value[4]));
                model.setTotalVoteMean(Double.parseDouble(value[5]));
                model.setTotalVoteStd(Double.parseDouble(value[6]));
                meanStdModelWrapper.meanStdModelMap.put(value[0], model);
            }
        });
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text value : values) {

            String[] line = value.toString().split("\\t");

            double newHelpfulVote = Double.parseDouble(line[8]);
            double newNotHelpfulVote = Double.parseDouble(line[9]);
            double newTotalVote = Double.parseDouble(line[10]);

            if (meanStdModelWrapper.meanStdModelMap.get(key.toString()).getHelpfulReviewStd() != 0) {
                newHelpfulVote = (Double.parseDouble(line[8]) -
                        meanStdModelWrapper.meanStdModelMap.get(key.toString()).getHelpfulReviewMean()) /
                        meanStdModelWrapper.meanStdModelMap.get(key.toString()).getHelpfulReviewStd();
            }

            if (meanStdModelWrapper.meanStdModelMap.get(key.toString()).getNotHelpfulReviewStd() != 0) {
                newNotHelpfulVote = (Double.parseDouble(line[9]) -
                        meanStdModelWrapper.meanStdModelMap.get(key.toString()).getNotHelpfulReviewMean()) /
                        meanStdModelWrapper.meanStdModelMap.get(key.toString()).getNotHelpfulReviewStd();
            }

            if (meanStdModelWrapper.meanStdModelMap.get(key.toString()).getTotalVoteStd() != 0) {
                newTotalVote = (Double.parseDouble(line[10]) -
                        meanStdModelWrapper.meanStdModelMap.get(key.toString()).getTotalVoteMean()) /
                        meanStdModelWrapper.meanStdModelMap.get(key.toString()).getTotalVoteStd();
            }

            if (newTotalVote != 0) {
                Text text = TextParser.HSeparatedString(line[0], line[1], line[2], line[3],
                        line[4], line[5], line[6], line[7], String.valueOf(newHelpfulVote),
                        String.valueOf(newNotHelpfulVote), String.valueOf(newTotalVote), line[10], line[11],
                        line[12], line[13], line[14], line[15]);
                context.write(NullWritable.get(), text);
            }

        }
    }

}
