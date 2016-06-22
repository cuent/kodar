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
import edu.ucuenca.kodar.utils.Printer;
import edu.ucuenca.kodar.utils.Writer;
import edu.ucuenca.kodar.utils.NameCluster;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.evaluation.ClusterEvaluator;
import org.apache.mahout.clustering.evaluation.RepresentativePointsDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;
import org.apache.mahout.math.Vector;

/**
 * Discover potential networks of collaboration and similar areas.
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class Clustering {

    private Date time;
    private String filePath;
    private boolean translate;

    private static final String BASE_PATH = System.getProperty("user.dir") + "/";
    private File KODAR_HOME;
    private final File RAW_DATA;
    private final File SEQUENCE_DATA;
    private final File SPARSE_VECTORS;
    private final File KMEANS;
    private final File SEED;
    private final File EVALUATION;
    private final File JOIN;
    private final File SORT;
    private final File RESULT;
    private String[] kmeans;
    private String[] seq2sparse;

    /**
     * The file to process should have the following headers.
     *
     * <code>name,authorUri,publicationUri,title,keywords</code>
     *
     * Results are stored in the KODAR HOME if provided otherwise are stored in
     * the locations of the project.
     *
     * @param filePath - Path of the file to process.
     * @param translate - true if you want to translate keywords.
     */
    public Clustering(String filePath, boolean translate) {
        this.filePath = filePath;
        this.translate = translate;

        String env = System.getenv("KODAR_HOME");
        if (env == null) {
            env = System.getProperty("user.dir") + "/kodar_home";
            KODAR_HOME = new File(env);
            if (!KODAR_HOME.exists()) {
                KODAR_HOME.mkdir();
            }
        }

        RAW_DATA = new File(KODAR_HOME, "raw");
        SEQUENCE_DATA = new File(KODAR_HOME, "sequence");
        SPARSE_VECTORS = new File(KODAR_HOME, "sparse");
        KMEANS = new File(KODAR_HOME, "kmeans");
        SEED = new File(KODAR_HOME, "seed");
        EVALUATION = new File(KODAR_HOME, "evaluation");
        JOIN = new File(KODAR_HOME, "join");
        SORT = new File(JOIN, "sort");
        RESULT = new File(KODAR_HOME, "result");

        if (!RESULT.exists()) {
            RESULT.mkdir();
        }

        loadConfigurations();
    }

    public void run() throws IOException, Exception {

        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://172.16.147.7:54310"); //set configuration of hadoop cluster 

        Controller controller = new ControllerImpl(conf);

        Printer printer = Printer.getReadSequenceFile();
        Writer writer = Writer.getWriteSequenceFile();
        ExportFileClusterig export = ExportFileClusterig.getExportFile();

        writer.disjoin(new File(filePath), RAW_DATA, translate);

        //Convert raw files to SequenceFile format
        controller.rawToSequenceTextKey(new File(RAW_DATA, "keywords.csv"), new Path(SEQUENCE_DATA.getPath(), "output"), ",");
        controller.rawToSequenceLongKey(new File(RAW_DATA, "keywords.csv"), new Path(SEQUENCE_DATA.getPath(), "outputLong"), ",");
        controller.rawToSequenceLongKey(new File(RAW_DATA, "keywords.csv"), new Path(SEQUENCE_DATA.getPath(), "outputAuthors"), ",");

        //Generate Sparse Vectors
        controller.seq2Sparse(seq2sparse);
        writer.writeVector(new Path(SPARSE_VECTORS.getPath(), "tfidf-vectors/part-r-00000"), new Path(SPARSE_VECTORS.getPath()));

        //Run KMeans
        controller.kmeans(kmeans);

//        evaluateCluster(conf, new Path(KMEANS.getPath(), "clusters-*-final/*"),
//                new Path(KMEANS.getPath(), "clusteredPoints"), 10, new Path(EVALUATION.getPath()));
        writer.writeClusterVector(new Path(KMEANS.getPath(), "clusteredPoints/part-m-0"),
                new Path(KMEANS.getPath()));
//        ClusterDumper clusterDumper = new ClusterDumper(new Path("mahout-base/kmeans/clusters-1-final"),
//                new Path("mahout-base/kmeans/clusteredPoints"));
//        clusterDumper.printClusters(null);

        Path clusteredPointsPath = new Path(KMEANS.getPath(), "clusteredPoints");
        Path outputFinalClustersPath = new Path(KMEANS.getPath(), "clusters-*-final/*");
        Path pointsToClusterPath = new Path(KMEANS.getPath(), "pointsToClusters");
        Path clusteredPostsPath = new Path(KMEANS.getPath(), "clusteredPosts");
        Path outputPostsPath = new Path(SEQUENCE_DATA.getPath(), "outputLong");
        Path resultOutputPath = new Path(JOIN.getPath());
        Path authorsPath = new Path(SEQUENCE_DATA.getPath(), "outputAuthors");

        //Delete File 
        HadoopUtil.delete(conf, pointsToClusterPath, clusteredPostsPath, resultOutputPath);

        // Join point name with cluster id
        PointToClusterMapperJob pointsToClusterMappingJob = new PointToClusterMapperJob(clusteredPointsPath, pointsToClusterPath);
        pointsToClusterMappingJob.setConf(conf);
        pointsToClusterMappingJob.mapPointsToClusters();

        //Join pointsToClusters with keywords
        ClusterJoinerMapperJob clusterJoinerJob = new ClusterJoinerMapperJob(outputPostsPath, pointsToClusterPath, clusteredPostsPath);
        clusterJoinerJob.setConf(conf);
        clusterJoinerJob.run();

        //Join clusteredKeywords with authors
        JoinKeywordsAuthorMapperJob joinKwAuthors = new JoinKeywordsAuthorMapperJob(clusteredPostsPath,
                authorsPath, resultOutputPath);
        joinKwAuthors.setConf(conf);
        joinKwAuthors.run();

        // Sort
        Path clusters = new Path(JOIN.getPath(), "part-00000");
        Path output = new Path(SORT.getPath());
        HadoopUtil.delete(conf, output);
        SortMapperJob sortByClusterId = new SortMapperJob(clusters, output);
        sortByClusterId.setConf(conf);
        sortByClusterId.run();

        // Label clusters
        NameCluster namedCluster = new NameCluster();
        namedCluster.setConf(conf);
        namedCluster.execute(new Path(SORT.getPath(), "part-r-00000"));

        export.writeResultFileCSV(KODAR_HOME + "/named-clusters", RESULT + "/final.csv");
        export.writeResultFileJSON(KODAR_HOME + "/named-clusters", RESULT + "/final.json");
        export.writeResultFileRDF(KODAR_HOME + "/named-clusters", RESULT + "/final.nt");
    }

    private void loadConfigurations() {
        seq2sparse = new String[]{
            "-i", new File(SEQUENCE_DATA, "output").getPath(),
            "-o", SPARSE_VECTORS.getPath(),
            "-x", "60",
            "-n", "2",
            "-ng", "2",
            "-wt", "tfidf",
            "-nv",
            "-ow"
        };

        kmeans = new String[]{
            "-i", new File(SPARSE_VECTORS, "tfidf-vectors").getPath(),
            "-o", KMEANS.getPath(),
            "-c", SEED.getPath(),
            "-dm", CosineDistanceMeasure.class.getName(),
            "-x", "100",
            "-k", "5",
            "-cl",
            "-xm", "sequential",
            "-ow"
        };
    }

    public void evaluateCluster(Configuration conf, Path clustersIn,
            Path clusteredPointsIn, int numIterations,
            Path output) throws InterruptedException, IOException, ClassNotFoundException {
        HadoopUtil.delete(conf, new Path(EVALUATION.getPath()));

        DistanceMeasure measure = new CosineDistanceMeasure();
        RepresentativePointsDriver.run(conf, clustersIn, clusteredPointsIn, output, measure,
                numIterations, false);

        RepresentativePointsDriver.printRepresentativePoints(output, numIterations);

        // var clustersIn
        ClusterEvaluator evaluator = new ClusterEvaluator(conf, new Path("mahout-base/kmeans/clusters-1-final"));
        System.out.println(evaluator.interClusterDensity());
        System.out.println(evaluator.intraClusterDensity());

        Map<Integer, Vector> distances = evaluator.interClusterDistances();

        for (Map.Entry<Integer, Vector> entry : distances.entrySet()) {
            int key = entry.getKey();
            Vector v = entry.getValue();

            System.out.println("key=" + key);
            System.out.println("value=" + v.asFormatString());
        }

        //<editor-fold defaultstate="collapsed" desc="Read clustered Points, which contains the final mapping from clusterId to documentId">
//        FileSystem fs = FileSystem.get(conf);
//        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(clusteredPointsIn, "part-m-0"), conf);
//
//        IntWritable key = new IntWritable();
//        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
//
//        while (reader.next(key, value)) {
//            NamedVector v = (NamedVector) value.getVector();
//            Iterator<Element> it = v.all().iterator();
//            System.out.println(v.getName());
//            System.out.print("[");
//            while (it.hasNext()) {
//                Element e = it.next();
//                System.out.print(e.get() + ",");
//            }
//            System.out.print("]\n");
//
//            Map<Text, Text> properties = value.getProperties();
//            for (Map.Entry<Text, Text> entry : properties.entrySet()) {
//                System.out.println("Key: " + entry.getKey().toString());
//                System.out.println("Value: " + entry.getValue().toString());
//            }
//            System.out.println(
//                    value.toString() + " belongs to cluster "
//                    + key.toString());
//        }
//</editor-fold>
    }

    public Date getTime() {
        return time;
    }

    public void setTime(Date time) {
        this.time = time;
    }

}
