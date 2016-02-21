/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.clusters;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;

/**
 *
 * @author cuent
 */
public class JoinKeywordsAuthorMapperJob extends Configured {

    private Path fileNamefinalCluster;
    private Path fileNameAuthors;
    private Path fileNameOutput;

    public JoinKeywordsAuthorMapperJob(Path fileNamefinalCluster, Path fileNameAuthors, Path fileNameOutput) {
        this.fileNamefinalCluster = fileNamefinalCluster;
        this.fileNameAuthors = fileNameAuthors;
        this.fileNameOutput = fileNameOutput;
    }

    public void run() throws IOException {
        Configuration configuration = this.getConf();

        JobConf job = new JobConf(configuration);
        job.setInputFormat(CompositeInputFormat.class);
        String strJoinStmt = CompositeInputFormat.compose("inner", SequenceFileInputFormat.class,
                fileNameAuthors, fileNamefinalCluster);
        job.set("mapred.join.expr", strJoinStmt);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, fileNameOutput);

        job.setMapperClass(KwDataJoiner.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(JoinKeywordsAuthorMapperJob.class);

        job.setJobName(JoinKeywordsAuthorMapperJob.class.getSimpleName());

        JobClient.runJob(new JobConf(job));
    }

    private static class KwDataJoiner extends MapReduceBase implements Mapper<LongWritable, TupleWritable, LongWritable, Text> {

        @Override
        public void map(LongWritable k, TupleWritable v, OutputCollector<LongWritable, Text> oc, Reporter rprtr) throws IOException {
            String[] data = v.get(0).toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);
            Text keyCluster = (Text) v.get(1);
            Text output;

            if (data.length == 4) {
                output = new Text(keyCluster.toString() + " Author: " + data[0] + " URI_A: " + data[1]
                        + " URI_P: " + data[2] + " Title: " + data[3]);
            } else if (data.length == 3) { //Author doesn't have a publication URI
                output = new Text(keyCluster.toString() + " Author: " + data[0] + " URI_A: " + data[1]
                        + " URI_P: " + " " + " Title: " + data[2]);
            } else if (data.length == 1) {
                output = new Text(keyCluster.toString() + " Author: " + data[0] + " URI_A: " + " "
                        + " URI_P: " + " " + " Title: " + " ");
            } else {
                throw new UnsupportedOperationException("Not supported yet.");
            }

            oc.collect(k, output);
        }

    }

    private static class AuthorKwJoiner extends MapReduceBase implements Mapper<LongWritable, TupleWritable, LongWritable, Text> {

        @Override
        public void map(LongWritable k, TupleWritable v, OutputCollector<LongWritable, Text> oc, Reporter rprtr) throws IOException {
            Text author = (Text) v.get(0);
            Text keyCluster = (Text) v.get(1);

            Text output = new Text(keyCluster.toString() + " Author: " + author.toString());

            oc.collect(k, output);
        }

    }
}
