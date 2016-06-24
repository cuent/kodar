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
package edu.ucuenca.kodar.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.clustering.iterator.ClusterWritable;
import org.apache.mahout.common.StringTuple;
import org.apache.mahout.math.VectorWritable;

/**
 * Sequence files are in a binary format. You could use the methods provided to
 * visualize the different sequence files.
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class Printer {
    // We should use reflection to improve the calls to this methods, the code is repeating to o much 

    private static Printer instanceRead = new Printer();

    private Printer() {
    }

    public static Printer getReadSequenceFile() {
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

    public <A extends Writable, B extends Writable> List<Tuple<A, B>> readSequenceFile(Path path, 
            Class<A> acls, Class<B> bcls) throws Exception {
        Configuration conf = new Configuration();
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
        long position = reader.getPosition();

        A key = acls.newInstance();
        B value = bcls.newInstance();

        List<Tuple<A, B>> results = new ArrayList<Tuple<A, B>>();
        while (reader.next(key, value)) {
            results.add(new Tuple(key, value));
            key = acls.newInstance();
            value = bcls.newInstance();
        }
        return results;
    }

    public <A extends Writable, B extends Writable> List<Tuple<A, B>> readSequenceFile(Path path, 
            Configuration conf, Class<A> acls, Class<B> bcls) throws Exception {
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
        long position = reader.getPosition();

        A key = acls.newInstance();
        B value = bcls.newInstance();

        List<Tuple<A, B>> results = new ArrayList<Tuple<A, B>>();
        while (reader.next(key, value)) {
            results.add(new Tuple(key, value));
            key = acls.newInstance();
            value = bcls.newInstance();
        }
        return results;
    }
}
