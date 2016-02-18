/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 *
 * @author cuent
 */
public class KODAWriter {

    private static KODAWriter instanceWriter = new KODAWriter();

    private KODAWriter() {
    }

    public static KODAWriter getWriteSequenceFile() {
        return instanceWriter;
    }

    public void writeClusterVector(String pathVectorFile, String pathToSave) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(pathVectorFile), conf);

        File file = new File(pathToSave + "clusters.csv");
        FileWriter fr = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fr);

        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        double valueElement = 0;
        while (reader.next(key, value)) {
            Iterator it = value.getVector().all().iterator();
            bw.write(key.toString() + ",");
            while (it.hasNext()) {
                Vector.Element element = (Vector.Element) it.next();
                valueElement = element.get();
                if (valueElement != 0.0) {
                    bw.write((valueElement * 100) + ",");
                } else {
                    bw.write(new Double(1.0) + ",");
                }
            }
            bw.write("\n");
        }
        reader.close();
        bw.flush();
        bw.close();
    }

    public void writeVector(String pathVectorFile, String pathToSave) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(pathVectorFile), conf);

        File file = new File(pathToSave + "tfidf.csv");
        FileWriter fr = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fr);

        Text key = new Text();
        VectorWritable value = new VectorWritable();
        double valueElement = 0;
        while (reader.next(key, value)) {
            Iterator it = value.get().all().iterator();
            while (it.hasNext()) {
                Vector.Element element = (Vector.Element) it.next();
                valueElement = element.get();
                if (valueElement != 0.0) {
                    bw.write((valueElement * 100) + ",");
                } else {
                    bw.write(new Double(1.0) + ",");
                }
            }
            bw.write("\n");
        }
        reader.close();
        bw.flush();
        bw.close();
    }

}
