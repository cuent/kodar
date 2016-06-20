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
import org.apache.mahout.clustering.lda.cvb.CVB0Driver;
import org.apache.mahout.utils.clustering.ClusterDumper;
import org.apache.mahout.utils.vectors.RowIdJob;
import org.apache.mahout.utils.vectors.VectorDumper;
import org.apache.mahout.vectorizer.SparseVectorsFromSequenceFiles;

/**
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
public class ControllerImpl implements Controller {

    private Configuration conf;

    public ControllerImpl(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void seqDirectoryToText(String inputFile, String outputFile) throws IOException {
        RawToSequence seq = new RawToSequence(conf, inputFile, outputFile, Text.class);
        seq.run();
    }

    @Override
    public void seqDirectoryToLong(String inputFile, String outputFile) throws IOException {
        RawToSequence seq = new RawToSequence(conf, inputFile, outputFile, LongWritable.class);
        seq.run();
    }

    @Override
    public void seq2Sparse(String[] seq2SparseArgs) throws Exception {
        ToolRunner.run(conf, new SparseVectorsFromSequenceFiles(), seq2SparseArgs);
    }

    @Override
    public void kmeans(String[] kmeansArgs) throws Exception {
        ToolRunner.run(conf, new KMeansDriver(), kmeansArgs);
    }

    @Override
    public void clusterDumper(String[] clusterDumperArgs) throws Exception {
        ToolRunner.run(conf, new ClusterDumper(), clusterDumperArgs);
    }

    @Override
    public void collapsedVariationalBayes(String[] args) throws Exception {
        ToolRunner.run(new CVB0Driver(), args);
    }

    @Override
    public void rowId(String[] rowIdArgs) throws Exception {
        ToolRunner.run(new RowIdJob(), rowIdArgs);
    }

    @Override
    public void vectorDump(String[] vectorDumpArgs) throws Exception {
        VectorDumper.main(vectorDumpArgs);
    }

}

class RawToSequence {

    private Configuration conf;
    private String inputFile;
    private String outputFile;
    private Class keyClass;

    public RawToSequence(Configuration conf, String inputFile, String outputFile, Class key) {
        this.conf = conf;
        this.inputFile = inputFile;
        this.outputFile = outputFile;
        this.keyClass = key;
    }

    public void run() throws IOException {
        Path path = new Path(outputFile);

        //opening file
        BufferedReader br = new BufferedReader(new FileReader(inputFile));
        //creating SequenceToLong writer
        FileSystem fs = FileSystem.get(conf);

        SequenceFile.Writer writer = null;
        writer = new SequenceFile.Writer(fs, conf, path, keyClass, Text.class);

        String line;
        String[] fields;
        String tempvalue;
        String delimiter = ",";

        Object key = (keyClass == Text.class) ? new Text() : new LongWritable();
        Text value = new Text();

        while ((line = br.readLine()) != null) {
            fields = line.split(delimiter);

            if (keyClass == Text.class) {
                ((Text) key).set(fields[0]);
            } else if (keyClass == LongWritable.class) {
                ((LongWritable) key).set(Long.parseLong(fields[0]));
            }

            tempvalue = "";
            for (int i = 1; i < fields.length; i++) {
                if (i == fields.length - 1) {
                    tempvalue += fields[i];
                } else {
                    tempvalue += fields[i] + delimiter;
                }
            }
            value.set(tempvalue);

            writer.append(key, value);
        }
        writer.close();
        br.close();
    }

}
