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
import java.io.File;
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

    private final Configuration conf;

    public ControllerImpl(Configuration conf) {
        this.conf = conf;
    }

    @Override
    public void rawToSequenceTextKey(File inputFile, Path outputFile, String delimiter) throws IOException {
        RawToSequence seq = new RawToSequence(conf);
        seq.convert(inputFile, outputFile, delimiter, Text.class);
    }

    @Override
    public void rawToSequenceLongKey(File inputFile, Path outputFile, String delimiter) throws IOException {
        RawToSequence seq = new RawToSequence(conf);
        seq.convert(inputFile, outputFile, delimiter, LongWritable.class);
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

/**
 * Converts a file in raw format in a Sequence file format.
 *
 * @author Xavier Sumba <xavier.sumba93@ucuenca.ec>
 */
class RawToSequence {

    private Configuration conf;

    public RawToSequence(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    /**
     * Convert Raw data to Sequence file format. Note: All values of the
     * Sequence file are of type Text.
     *
     * @param inputFile path of the raw file, where each line is a tuple.
     * @param outputFile path to store the sequence file.
     * @param delimiter separator for each record in a line.
     * @param keyClass Class for for the key of the sequence file.
     * @throws IOException
     */
    public void convert(File inputFile, Path outputFile, String delimiter, Class keyClass) throws IOException {
        FileSystem fs = FileSystem.get(conf);

        // Use try-with resources to automatically close br and writer>
        try (BufferedReader br = new BufferedReader(new FileReader(inputFile)); // Open raw file
                SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, outputFile, keyClass, Text.class)) {

            String line;
            String[] fields;
            String tempvalue;

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
        }
    }
}
