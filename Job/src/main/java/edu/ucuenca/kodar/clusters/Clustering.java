/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.clusters;

import edu.uc.mahout.base.topicmodel.SortMapperJob;
import edu.ucuenca.kodar.utils.ExportFileClusterig;
import edu.ucuenca.kodar.utils.KODAWriter;
import edu.ucuenca.kodar.utils.KODAReader;
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
    private String pathAuthors;
    private String pathKeywords;
    private static final String FOLDER = System.getProperty("user.dir") + "/";

    public Clustering(String pathAuthors, String pathKeywords) {
        this.pathAuthors = pathAuthors;
        this.pathKeywords = pathKeywords;
    }

    public void run() throws IOException, Exception {
        KODAReader reader = KODAReader.getReadSequenceFile();
        KODAWriter writer = KODAWriter.getWriteSequenceFile();
        ExportFileClusterig export = ExportFileClusterig.getExportFile();

        MahoutController mahout = new MahoutController();
        //Hadoop Configuration
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://172.16.147.7:54310"); //set configuration of hadoop cluster

        //Convert raw files to SequenceFile format
        mahout.seqDirectoryToText(conf, pathKeywords, FOLDER + "mahout-base/sequence/output");
        //mr.readTT("mahout-base/sequence/output");
        mahout.seqDirectoryToLong(conf, pathKeywords, FOLDER + "mahout-base/sequence/outputLong");
        //mr.readLT("mahout-base/sequence/outputLong");
        mahout.seqDirectoryToLong(conf, pathAuthors, FOLDER + "mahout-base/sequence/outputAuthors");
        //mr.readLT("mahout-base/sequence/outputAuthors");

        //Generate Sparse Vectors
        String[] seq2sparse = new String[]{
            "-i", "mahout-base/sequence/output",
            "-o", "mahout-base/sparse",
            "-x", "60",
            "-n", "2",
            "-ng", "2",
            "-wt", "tfidf",
            "-nv",
            "-ow"
        };
        mahout.seq2Sparse(conf, seq2sparse);
        //mr.readTI("mahout-base/sparse/dictionary.file-0");
        writer.writeVector("mahout-base/sparse/tfidf-vectors/part-r-00000", "mahout-base/sparse/");
        //mr.readTL("mahout-base/sparse/wordcount/ngrams/part-r-00000");
        //mr.readTL("mahout-base/sparse/wordcount/subgrams/part-r-00000");

        //Run KMeans
        String[] kmeans = new String[]{
            "-i", "mahout-base/sparse/tfidf-vectors",
            "-o", "mahout-base/kmeans",
            "-c", "mahout-base/seed",
            "-dm", CosineDistanceMeasure.class.getName(),
            "-x", "100",
            "-k", "2000",
            "-cl",
            "-xm", "sequential",
            "-ow"

        };
        mahout.kmeans(conf, kmeans);
        reader.readTC("mahout-base/seed/part-randomSeed");
        reader.readIW("mahout-base/kmeans/clusteredPoints/part-m-0");
        writer.writeClusterVector("mahout-base/kmeans/clusteredPoints/part-m-0", "mahout-base/kmeans/");
        //mr.readIC("mahout-base/kmeans/clusters-1-final/part-00000");
        //mr.readIC("mahout-base/kmeans/clusters-1-final/part-00001");
        //ClusterDumper clusterDumper = new ClusterDumper(new Path(
//        "mahout-base/kmeans/clusters-1-final"
//        ), new Path(
//                "mahout-base/kmeans/clusteredPoints")
//        );
        //clusterDumper.printClusters(null);

        Path outputClusteringPath = new Path("mahout-base/", "kmeans");
        Path clusteredPointsPath = new Path(outputClusteringPath, "clusteredPoints");
        Path outputFinalClustersPath = new Path(outputClusteringPath, "clusters-*-final/*");
        Path pointsToClusterPath = new Path("mahout-base/", "pointsToClusters");
        Path clusteredPostsPath = new Path("mahout-base/", "clusteredPosts");
        Path outputPostsPath = new Path("mahout-base/sequence/outputLong");
        Path resultOutputPath = new Path("mahout-base/", "result");
        Path authorsPath = new Path("mahout-base/sequence/outputAuthors");

        //Delete File 
        HadoopUtil.delete(conf, pointsToClusterPath, clusteredPostsPath, resultOutputPath);

        //Join point name with cluster id
        PointToClusterMapperJob pointsToClusterMappingJob = new PointToClusterMapperJob(clusteredPointsPath, pointsToClusterPath);
        pointsToClusterMappingJob.setConf(conf);
        pointsToClusterMappingJob.mapPointsToClusters();

        reader.readLI("mahout-base/pointsToClusters/part-r-00000");

        //Join pointsToClusters with keywords
        ClusterJoinerMapperJob clusterJoinerJob = new ClusterJoinerMapperJob(outputPostsPath, pointsToClusterPath, clusteredPostsPath);
        clusterJoinerJob.setConf(conf);
        clusterJoinerJob.run();

        reader.readLT("mahout-base/clusteredPosts/part-00000");

        //Join clusteredKeywords with authors
        JoinKeywordsAuthorMapperJob joinKwAuthors = new JoinKeywordsAuthorMapperJob(clusteredPostsPath,
                authorsPath, resultOutputPath);
        joinKwAuthors.setConf(conf);
        joinKwAuthors.run();

        reader.readLT("mahout-base/result/part-00000");

        Path clusters = new Path("mahout-base/result/part-00000");
        Path output = new Path("mahout-base/", "sort");
        HadoopUtil.delete(conf, output);
        SortMapperJob sortByClusterId = new SortMapperJob(clusters, output);
        sortByClusterId.setConf(conf);
        sortByClusterId.run();

        NameCluster namedCluster = new NameCluster();
        namedCluster.execute("mahout-base/sort/part-r-00000");

        export.writeResultFileCSV("mahout-base/named-clusters", "mahout-base/final.csv");
        //export.writeResultFileCSV("mahout-base/result/part-00000", "mahout-base/final.csv");
        export.writeResultFileJSON("mahout-base/named-clusters", "mahout-base/final.json");
        export.writeResultFileRDF("mahout-base/named-clusters", "mahout-base/final.rdf");
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

}
