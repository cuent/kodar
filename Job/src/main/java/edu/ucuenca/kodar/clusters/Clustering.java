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
import edu.ucuenca.kodar.utils.NameCluster;
import edu.ucuenca.kodar.utils.Writer;
import java.io.File;
import java.io.IOException;
import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.clustering.evaluation.ClusterEvaluator;
import org.apache.mahout.clustering.evaluation.RepresentativePointsDriver;
import org.apache.mahout.common.HadoopUtil;
import org.apache.mahout.common.distance.CosineDistanceMeasure;
import org.apache.mahout.common.distance.DistanceMeasure;

/**
 * Discover potential networks of collaboration and similar knowledge areas.
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class Clustering {

    private Date time;
    private String datasetPath;
    private boolean translate = false;
    private boolean evaluate = false;
    private double interClusterDensity;

    private final Controller controller;
    private final Writer writer = Writer.getWriteSequenceFile();
    private final ExportFileClusterig export = ExportFileClusterig.getExportFile();
    private Configuration conf = null;

    private File KODAR_HOME;
    private final File RAW_DATA;
    private final File SEQUENCE_DATA;
    private final File SPARSE_VECTORS;
    private final File KMEANS;
    private final File SEED;
    private final File EVALUATION;
    private final File RESULT;
    private final File MR_JOBS;
    private final Path POINTS_TO_CLUSTERS;
    private final Path CLUSTER_KEYWORDS;
    private final Path RESULT_OUTPUT;
    private final Path SORT;

    /**
     * The file to process should have the following headers.
     *
     * <code>name,authorUri,publicationUri,title,keywords</code>
     *
     * Results are stored in the KODAR HOME if provided otherwise are stored in
     * the project's folder.
     *
     * @param datasetPath Path of the file to process.
     */
    public Clustering(String datasetPath) {
        this.datasetPath = datasetPath;

        String env = System.getenv("KODAR_HOME");
        if (env == null) {
            env = System.getProperty("user.dir") + "/target/kodar_home";
            KODAR_HOME = new File(env);
            if (!KODAR_HOME.exists()) {
                KODAR_HOME.mkdir();
            }
        }

        RAW_DATA = new File(KODAR_HOME, "raw");
        SEQUENCE_DATA = new File(KODAR_HOME, "sequence");
        SPARSE_VECTORS = new File(KODAR_HOME, "sparse");
        KMEANS = new File(KODAR_HOME, "kmeans");
        SEED = new File(KMEANS, "seed");
        EVALUATION = new File(KODAR_HOME, "evaluation");
        RESULT = new File(KODAR_HOME, "result");
        MR_JOBS = new File(KODAR_HOME, "mr_jobs");
        POINTS_TO_CLUSTERS = new Path(MR_JOBS.getPath(), "points_to_clusters");
        CLUSTER_KEYWORDS = new Path(MR_JOBS.getPath(), "clusteredKeywords");
        RESULT_OUTPUT = new Path(MR_JOBS.getPath(), "clusteredData");
        SORT = new Path(MR_JOBS.getPath(), "sort");

        if (!RESULT.exists()) {
            RESULT.mkdir();
        }

        if (conf == null) {
            conf = new Configuration();
            //conf.set("fs.defaultFS", "hdfs://172.16.147.7:54310"); //set configuration of hadoop cluster 
        }
        controller = new ControllerImpl(conf);
    }

    public void run() throws IOException, Exception {
        preprocessData();
        generateSparseVectors();
        executeKmeans();
        executeFuzzyKmeans();

        writer.writeClusterVector(new Path(KMEANS.getPath(), "clusteredPoints/part-m-0"),
                new Path(KMEANS.getPath()));

//        ClusterDumper clusterDumper = new ClusterDumper(new Path("kodar_home/kmeans/clusters-1-final"),
//                new Path("kodar_home/kmeans/clusteredPoints"));
//        clusterDumper.printClusters(null);
        //Path outputFinalClustersPath = new Path(KMEANS.getPath(), "clusters-*-final/*");
        joinCLusterResults();

        // Label clusters
        NameCluster namedCluster = new NameCluster();
        namedCluster.setConf(conf);
        namedCluster.execute(SORT);

        export.writeResultFileCSV(KODAR_HOME + "/named-clusters", RESULT + "/final.csv");
        export.writeResultFileJSON(KODAR_HOME + "/named-clusters", RESULT + "/final.json");
        export.writeResultFileRDF(KODAR_HOME + "/named-clusters", RESULT + "/final.nt");

        if (evaluate) {
            evaluateCluster(conf);
        }
    }

    private void preprocessData() throws IOException {
        // Preprocess data
        writer.disjoin(new File(datasetPath), RAW_DATA, translate);

        controller.rawToSequenceTextKey(new File(RAW_DATA, "keywords.csv"), new Path(SEQUENCE_DATA.getPath(), "output"), ",");
        controller.rawToSequenceLongKey(new File(RAW_DATA, "keywords.csv"), new Path(SEQUENCE_DATA.getPath(), "outputLong"), ",");
        controller.rawToSequenceLongKey(new File(RAW_DATA, "authors.csv"), new Path(SEQUENCE_DATA.getPath(), "outputAuthors"), ",");
    }

    private void generateSparseVectors() throws Exception {
        // Generate Sparse Vectors
        String[] seq2sparse = new String[]{
            "-i", new File(SEQUENCE_DATA, "output").getPath(),
            "-o", SPARSE_VECTORS.getPath(),
            "-x", "60",
            "-n", "2",
            "-ng", "2",
            "-wt", "tfidf",
            "-nv",
            "-ow"
        };

        controller.seq2Sparse(seq2sparse);
        writer.writeVector(new Path(SPARSE_VECTORS.getPath(), "tfidf-vectors/part-r-00000"), new Path(SPARSE_VECTORS.getPath()));
    }

    private void executeKmeans() throws Exception {
        // Run KMeans
        String[] kmeans = new String[]{
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
        controller.kmeans(kmeans);
    }

    private void executeFuzzyKmeans() {

        String[] fuzzykmeans = new String[]{
            "-i", "<input vectors directory>",
            "-c", "<input clusters directory>",
            "-o", "<output working directory>",
            "-dm", "<DistanceMeasure>",
            "-m", "<fuzziness argument >1>",
            "-x", "<maximum number of iterations>",
            "-k", "<optional number of initial clusters to sample from input vectors>",
            "-cd", "<optional convergence delta. Default is 0.5>",
            "-ow", "<overwrite output directory if present>",
            "-cl", "<run input vector clustering after computing Clusters>",
            "-e", "<emit vectors to most likely cluster during clustering>",
            "-t", "<threshold to use for clustering if -e is false>",
            "-xm", "<execution method: sequential or mapreduce>"
        };
    }

    private void joinCLusterResults() throws IOException, ClassNotFoundException, InterruptedException, Exception {
        // Delete File 
        HadoopUtil.delete(conf, new Path(MR_JOBS.getPath()));

        // Join point name with cluster id
        PointToClusterMapperJob pointsToClusterMappingJob = new PointToClusterMapperJob(new Path(KMEANS.getPath(), "clusteredPoints"),
                POINTS_TO_CLUSTERS);
        pointsToClusterMappingJob.setConf(conf);
        pointsToClusterMappingJob.mapPointsToClusters();

        // Join keywords with pointsToClusters
        ClusterJoinerMapperJob clusterJoinerJob = new ClusterJoinerMapperJob(new Path(SEQUENCE_DATA.getPath(), "outputLong"),
                POINTS_TO_CLUSTERS, CLUSTER_KEYWORDS);
        clusterJoinerJob.setConf(conf);
        clusterJoinerJob.run();

        // Join clusteredKeywords with authors
        JoinKeywordsAuthorMapperJob joinKwAuthors = new JoinKeywordsAuthorMapperJob(CLUSTER_KEYWORDS,
                new Path(SEQUENCE_DATA.getPath(), "outputAuthors"), RESULT_OUTPUT);
        joinKwAuthors.setConf(conf);
        joinKwAuthors.run();

        // Sort and group
        SortMapperJob sortByClusterId = new SortMapperJob(RESULT_OUTPUT, SORT);
        sortByClusterId.setConf(conf);
        sortByClusterId.run();
    }

    private void evaluateCluster(Configuration conf) throws InterruptedException, IOException, ClassNotFoundException {
        HadoopUtil.delete(conf, new Path(EVALUATION.getPath()));

        DistanceMeasure measure = new CosineDistanceMeasure();
        int numIterations = 2;
        Path clustersIn = new Path(KMEANS.getPath(), "clusters-*-final/*");
        Path clusteredPointsIn = new Path(KMEANS.getPath(), "clusteredPoints");
        RepresentativePointsDriver.run(conf, clustersIn, clusteredPointsIn, new Path(EVALUATION.getPath()), measure,
                numIterations, false);
        //RepresentativePointsDriver.printRepresentativePoints(new Path(EVALUATION.getPath()), numIterations);
        // var clustersIn
        ClusterEvaluator evaluator = new ClusterEvaluator(conf, new Path(KMEANS.getPath(), "clusters-1-final"));
        interClusterDensity = evaluator.interClusterDensity();
//        System.out.println(evaluator.interClusterDensity());
//        System.out.println(evaluator.intraClusterDensity());
//
//        Map<Integer, Vector> distances = evaluator.interClusterDistances();
//
//        for (Map.Entry<Integer, Vector> entry : distances.entrySet()) {
//            int key = entry.getKey();
//            Vector v = entry.getValue();
//
//            System.out.println("key=" + key);
//            System.out.println("value=" + v.asFormatString());
//        }

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

    /**
     * Return the path of the dataset to been executed.
     *
     * @return
     */
    public String getDatasetPath() {
        return datasetPath;
    }

    /**
     * In case you want to change the path of the dataset to be executed.
     *
     * @param datasetPath
     */
    public void setDatasetPath(String datasetPath) {
        this.datasetPath = datasetPath;
    }

    public boolean isTranslate() {
        return translate;
    }

    /**
     * Set <code>true</code> if you want to translate the keywords field.
     *
     * @param translate
     */
    public void setTranslate(boolean translate) {
        this.translate = translate;
    }

    /**
     * Return true if it is executing in evaluate mode.
     *
     * @return
     */
    public boolean isEvaluation() {
        return evaluate;
    }

    /**
     * Set <code>true</code> to execute en mode evaluate. Mode evaluate executes
     * a ClusterEvaluation to know the inter-cluster density.
     *
     * @param evaluation
     */
    public void setEvaluation(boolean evaluation) {
        this.evaluate = evaluation;
    }

    /**
     * Returns the Inter-cluster density. Inter-cluster distance is a good
     * measure of clustering quality; good clusters probably don’t have
     * centroids that are too close to each other, because this would indicate
     * that the clustering process is creating groups with similar features, and
     * perhaps drawing distinctions between cluster members that are hard to
     * support.
     *
     * Remember to set true the evaluate.
     *
     * @return
     */
    public double getInterClusterDensity() {
        return interClusterDensity;
    }

}
