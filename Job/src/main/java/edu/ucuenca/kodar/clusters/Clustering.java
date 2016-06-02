/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucuenca.kodar.clusters;

import edu.uc.mahout.base.topicmodel.SortMapperJob;
import edu.ucuenca.kodar.utils.ExportFileClusterig;
import edu.ucuenca.kodar.utils.KODAReader;
import edu.ucuenca.kodar.utils.KODAWriter;
import edu.ucuenca.kodar.utils.NameCluster;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;

/**
 *
 * @author cuent
 */
public class Clustering {

    private Date time;
    private String pathOriginal;
    private boolean translate;
    private static final String BASE_PATH = System.getProperty("user.dir") + "/";

    public Clustering(String pathAuthors, boolean translate) {
        this.pathOriginal = pathAuthors;
        this.translate = translate;
    }

    public void run() throws IOException, Exception {
        KODAReader reader = KODAReader.getReadSequenceFile();
        KODAWriter writer = KODAWriter.getWriteSequenceFile();
        ExportFileClusterig export = ExportFileClusterig.getExportFile();

        writer.disjoin(pathOriginal,translate);

        MahoutController mahout = new MahoutController();

        //Hadoop Configuration
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://172.16.147.7:54310"); //set configuration of hadoop cluster

        //Convert raw files to SequenceFile format
        mahout.seqDirectoryToText(conf, BASE_PATH + "mahout-base/original/keywords.csv", BASE_PATH + "mahout-base/sequence/output");
        //mr.readTT("mahout-base/sequence/output");
        mahout.seqDirectoryToLong(conf, BASE_PATH + "mahout-base/original/keywords.csv", BASE_PATH + "mahout-base/sequence/outputLong");
        //mr.readLT("mahout-base/sequence/outputLong");
        mahout.seqDirectoryToLong(conf, BASE_PATH + "mahout-base/original/authors.csv", BASE_PATH + "mahout-base/sequence/outputAuthors");
        //mr.readLT("mahout-base/sequence/outputAuthors");

        //Generate Sparse Vectors
        String[] seq2sparse = new String[]{
            "-i", BASE_PATH + "mahout-base/sequence/output",
            "-o", BASE_PATH + "mahout-base/sparse",
            "-x", "60",
            "-n", "2",
            "-ng", "2",
            "-wt", "tfidf",
            "-nv",
            "-ow"
        };
        mahout.seq2Sparse(conf, seq2sparse);
        //mr.readTI("mahout-base/sparse/dictionary.file-0");
        writer.writeVector(BASE_PATH + "mahout-base/sparse/tfidf-vectors/part-r-00000", BASE_PATH + "mahout-base/sparse/");
        //mr.readTL("mahout-base/sparse/wordcount/ngrams/part-r-00000");
        //mr.readTL("mahout-base/sparse/wordcount/subgrams/part-r-00000");

        //Run KMeans
        String[] kmeans = new String[]{
            "-i", BASE_PATH + "mahout-base/sparse/tfidf-vectors",
            "-o", BASE_PATH + "mahout-base/kmeans",
            "-c", BASE_PATH + "mahout-base/seed",
            "-dm", CosineDistanceMeasure.class.getName(),
            "-x", "100",
            "-k", "100",
            "-cl",
            "-xm", "sequential",
            "-ow"

        };
        mahout.kmeans(conf, kmeans);
        //reader.readTC("mahout-base/seed/part-randomSeed");
        //reader.readIW("mahout-base/kmeans/clusteredPoints/part-m-0");
        writer.writeClusterVector(BASE_PATH + "mahout-base/kmeans/clusteredPoints/part-m-0",
                BASE_PATH + "mahout-base/kmeans/");
        //mr.readIC("mahout-base/kmeans/clusters-1-final/part-00000");
        //mr.readIC("mahout-base/kmeans/clusters-1-final/part-00001");
        //ClusterDumper clusterDumper = new ClusterDumper(new Path(
//        "mahout-base/kmeans/clusters-1-final"
//        ), new Path(
//                "mahout-base/kmeans/clusteredPoints")
//        );
        //clusterDumper.printClusters(null);

        Path outputClusteringPath = new Path(BASE_PATH + "mahout-base/", "kmeans");
        Path clusteredPointsPath = new Path(outputClusteringPath, "clusteredPoints");
        Path outputFinalClustersPath = new Path(outputClusteringPath, "clusters-*-final/*");
        Path pointsToClusterPath = new Path(BASE_PATH + "mahout-base/", "pointsToClusters");
        Path clusteredPostsPath = new Path(BASE_PATH + "mahout-base/", "clusteredPosts");
        Path outputPostsPath = new Path(BASE_PATH + "mahout-base/sequence/outputLong");
        Path resultOutputPath = new Path(BASE_PATH + "mahout-base/", "result");
        Path authorsPath = new Path(BASE_PATH + "mahout-base/sequence/outputAuthors");

        //Delete File 
        HadoopUtil.delete(conf, pointsToClusterPath, clusteredPostsPath, resultOutputPath);

        //Join point name with cluster id
        PointToClusterMapperJob pointsToClusterMappingJob = new PointToClusterMapperJob(clusteredPointsPath, pointsToClusterPath);
        pointsToClusterMappingJob.setConf(conf);
        pointsToClusterMappingJob.mapPointsToClusters();

        //reader.readLI("mahout-base/pointsToClusters/part-r-00000");
        //Join pointsToClusters with keywords
        ClusterJoinerMapperJob clusterJoinerJob = new ClusterJoinerMapperJob(outputPostsPath, pointsToClusterPath, clusteredPostsPath);
        clusterJoinerJob.setConf(conf);
        clusterJoinerJob.run();

        //reader.readLT("mahout-base/clusteredPosts/part-00000");
        //Join clusteredKeywords with authors
        JoinKeywordsAuthorMapperJob joinKwAuthors = new JoinKeywordsAuthorMapperJob(clusteredPostsPath,
                authorsPath, resultOutputPath);
        joinKwAuthors.setConf(conf);
        joinKwAuthors.run();

        //reader.readLT("mahout-base/result/part-00000");
        Path clusters = new Path(BASE_PATH + "mahout-base/result/part-00000");
        Path output = new Path(BASE_PATH + "mahout-base/", "sort");
        HadoopUtil.delete(conf, output);
        SortMapperJob sortByClusterId = new SortMapperJob(clusters, output);
        sortByClusterId.setConf(conf);
        sortByClusterId.run();

        NameCluster namedCluster = new NameCluster();
        namedCluster.setConf(conf);
        namedCluster.execute(BASE_PATH + "mahout-base/sort/part-r-00000");

        export.writeResultFileCSV(BASE_PATH + "mahout-base/named-clusters", "mahout-base/final.csv");
        //export.writeResultFileCSV("mahout-base/result/part-00000", "mahout-base/final.csv");
        export.writeResultFileJSON(BASE_PATH + "mahout-base/named-clusters", "mahout-base/final.json");
        export.writeResultFileRDF(BASE_PATH + "mahout-base/named-clusters", "mahout-base/final.rdf");
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

}
