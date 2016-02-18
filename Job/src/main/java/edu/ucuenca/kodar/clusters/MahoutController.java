/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.clusters;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ToolRunner;
import org.apache.mahout.clustering.kmeans.KMeansDriver;
import org.apache.mahout.clustering.lda.LDADriver;
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.utils.vectors.RowIdJob;
import org.apache.mahout.utils.vectors.VectorDumper;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

/**
 *
 * @author cuent
 */
public class MahoutController {

    public void seqDirectoryToText(Configuration conf, String inputFile, String outputFile) throws IOException {
        SequenceToText seq = new SequenceToText(conf, inputFile, outputFile);
        seq.run();
    }

    public void seqDirectoryToLong(Configuration conf, String inputFile, String outputFile) throws IOException {
        SequenceToLong seq = new SequenceToLong(conf, inputFile, outputFile);
        seq.run();
    }

    public void seq2Sparse(Configuration conf, String[] seq2SparseArgs) throws Exception {
        ToolRunner.run(conf, new SparseVectorsFromSequenceFiles(), seq2SparseArgs);
    }

    public void kmeans(Configuration conf, String[] kmeansArgs) throws Exception {
        ToolRunner.run(conf, new KMeansDriver(), kmeansArgs);
    }

    public void clusterDumper(Configuration conf, String[] clusterDumperArgs) throws Exception {
        ToolRunner.run(conf, new ClusterDumper(), clusterDumperArgs);
    }

    public void collapsedVariationalBayes(Configuration conf, String[] args) throws Exception {
        ToolRunner.run(conf, new CVB0Driver(), args);
    }

    public void rowId(Configuration conf, String[] rowIdArgs) throws Exception {
        ToolRunner.run(new RowIdJob(), rowIdArgs);
    }

    public void vectorDump(String[] vectorDumpArgs) throws Exception {
        VectorDumper.main(vectorDumpArgs);
    }

    public void seqDumper() {
        //Implementar codigo para leer ficheros tipo SequenceFile
    }
}

class SequenceToText {

    private Configuration conf;
    private String inputFile;
    private String outputFile;

    public SequenceToText(Configuration conf, String inputFile, String outputFile) {
        this.conf = conf;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    public void run() throws IOException {
        Path path = new Path(outputFile);

        //opening file
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        //creating SequenceToLong writer
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, Text.class, Text.class);

        String line;
        String[] temp;
        String tempvalue = new String();
        String delimiter = ",";
        String espace = " ";
        Text key;
        Text value = new Text();
        long tempkey = 0;

        while ((line = br.readLine()) != null) {
            tempkey++;
            //line = br.readLine();
            temp = line.split(delimiter);

            //key = new Text(tempkey + "");
            key = new Text(temp[0]);
            value = new Text();
            tempvalue = "";
            for (int i = 1; i < temp.length; i++) {
                if (i == temp.length - 1) {
                    tempvalue += temp[i] + espace;
                } else {
                    tempvalue += temp[i] + delimiter;
                }
            }
            value = new Text(tempvalue);

            //System.out.println("writing key/value " + key.toString() + "/" + value.toString());
            writer.append(key, value);
        }
        writer.close();
        br.close();
    }

}

class SequenceToLong {

    private Configuration conf;
    private String inputFile;
    private String outputFile;

    public SequenceToLong(Configuration conf, String inputFile, String outputFile) {
        this.conf = conf;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
    }

    public void run() throws IOException {
        Path path = new Path(outputFile);

        //opening file
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        //creating SequenceToLong writer
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, path, LongWritable.class, Text.class);

        String line;
        String[] temp;
        String tempvalue = new String();
        String delimiter = ",";
        String espace = " ";
        LongWritable key;
        Text value = new Text();
        long tempkey = 0;

        while ((line = br.readLine()) != null) {
            tempkey++;
            //line = br.readLine();
            temp = line.split(delimiter);

            //key = new Text(tempkey + "");
            key = new LongWritable(Long.parseLong(temp[0]));
            value = new Text();
            tempvalue = "";
            for (int i = 1; i < temp.length; i++) {
                if (i == temp.length - 1) {
                    tempvalue += temp[i] + espace;
                } else {
                    tempvalue += temp[i] + delimiter;
                }
            }
            value = new Text(tempvalue);

            //System.out.println("writing key/value " + key.toString() + "/" + value.toString());
            writer.append(key, value);
        }
        writer.close();
        br.close();
    }

}
