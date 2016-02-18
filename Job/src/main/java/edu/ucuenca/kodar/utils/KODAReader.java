/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

import com.google.gson.Gson;
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
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VectorWritable;
import org.apache.mahout.math.Vector;

/**
 *
 * @author cuent
 */
public class KODAReader {

    private static KODAReader instanceRead = new KODAReader();

    private KODAReader() {
    }

    public static KODAReader getReadSequenceFile() {
        return instanceRead;
    }

    public void readTT(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        Text v = new Text();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readTI(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        IntWritable v = new IntWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readTL(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        LongWritable v = new LongWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readTS(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        StringTuple v = new StringTuple();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readTV(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        VectorWritable v = new VectorWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readIL(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        IntWritable k = new IntWritable();
        LongWritable v = new LongWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readTC(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        ClusterWritable v = new ClusterWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readIC(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        IntWritable k = new IntWritable();
        ClusterWritable v = new ClusterWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readIW(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        IntWritable k = new IntWritable();
        WeightedPropertyVectorWritable v = new WeightedPropertyVectorWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readLT(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        LongWritable k = new LongWritable();
        Text v = new Text();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

    public void readLI(String pathFileInput) throws IOException {
        Path path = new Path(pathFileInput);

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        LongWritable k = new LongWritable();
        IntWritable v = new IntWritable();

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            System.out.println("reading key: " + k.toString() + " with value "
                    + v.toString());
        }
    }

}
