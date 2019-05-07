package com.bigdata.finalproject.recommendation;

import com.bigdata.finalproject.InitConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class MahoutRecommendation {

    private volatile static MahoutRecommendation mahoutRecommendation;

    private Path TMP_PATH = new Path("/tmp/processeddata");
    private Path inputPath;
    private Recommender recommender;

    private Configuration conf;

    private MahoutRecommendation() {
    }

    public static MahoutRecommendation getInstance() {
        if (mahoutRecommendation == null) {
            synchronized (MahoutRecommendation.class) {
                if (mahoutRecommendation == null)
                    mahoutRecommendation = new MahoutRecommendation();
            }
        }
        return mahoutRecommendation;
    }

    Recommender getRecommender() {
        return recommender;
    }

    public void setInputPath(Path inputPath) {
        this.inputPath = inputPath;
    }

    public void prepareModel() {
        try {

            conf = InitConfig.getConfiguration();
            FileSystem hdfs = FileSystem.get(conf);
            FileSystem local = FileSystem.getLocal(conf);

            if (local.exists(TMP_PATH))
                local.delete(TMP_PATH, true);
            local.createNewFile(TMP_PATH);

            FileStatus[] inputFiles = hdfs.listStatus(inputPath);
            FSDataOutputStream localOut = local.create(TMP_PATH);

            Arrays.stream(inputFiles).forEach(inputFile -> {
                try (FSDataInputStream hdfsIn = hdfs.open(inputFile.getPath())) {
                    byte[] buffer = new byte[256];
                    int bytesRead = 0;
                    while ((bytesRead = hdfsIn.read(buffer)) > 0) {
                        localOut.write(buffer, 0, bytesRead);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            localOut.close();
            local.close();

            DataModel model = new FileDataModel(new File(TMP_PATH.toString()));
            UserSimilarity userSimilarity = new PearsonCorrelationSimilarity(model);

            UserNeighborhood userNeighborhood = new NearestNUserNeighborhood(5, userSimilarity, model);

            recommender = new GenericUserBasedRecommender(model, userNeighborhood, userSimilarity);

        } catch (IOException | TasteException e) {
            e.printStackTrace();
        }
    }

}
