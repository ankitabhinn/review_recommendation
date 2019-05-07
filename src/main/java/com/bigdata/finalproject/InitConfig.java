package com.bigdata.finalproject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class InitConfig {

    public static Configuration getConfiguration() {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://localhost:9000");
        configuration.addResource(new Path("/usr/local/bin/hadoop-3.2.0/etc/hadoop/core-site.xml"));
        configuration.addResource(new Path("/usr/local/bin/hadoop-3.2.0/etc/hadoop/hdfs-site.xml"));

        // Because of Maven
        configuration.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        configuration.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());

        return configuration;
    }

}
