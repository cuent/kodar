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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.join.CompositeInputFormat;
import org.apache.hadoop.mapred.join.TupleWritable;

import java.io.IOException;
import org.apache.hadoop.io.Text;

public class ClusterJoinerMapperJob extends Configured {

    private Path clusteredPointsPath;
    private Path documentPath;
    private Path outputPath;

    public ClusterJoinerMapperJob(Path postPath, Path clusteredPointsPath, Path outputPath) {
        this.clusteredPointsPath = clusteredPointsPath;
        this.documentPath = postPath;
        this.outputPath = outputPath;
    }

    public void run() throws Exception {
        Configuration configuration = getConf();

        JobConf job = new JobConf(configuration);
        job.setInputFormat(CompositeInputFormat.class);
        String strJoinStmt = CompositeInputFormat.compose("inner", SequenceFileInputFormat.class,
                documentPath, clusteredPointsPath);
        job.set("mapred.join.expr", strJoinStmt);
        job.setOutputFormat(SequenceFileOutputFormat.class);

        FileOutputFormat.setOutputPath(job, outputPath);

        job.setMapperClass(ClusterJoinMapper.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);
        job.setJarByClass(ClusterJoinerMapperJob.class);

        job.setJobName(ClusterJoinerMapperJob.class.getSimpleName());

        JobClient.runJob(new JobConf(job));
    }

    private static class ClusterJoinMapper extends MapReduceBase implements Mapper<LongWritable, TupleWritable, LongWritable, Text> {
        //private ClusteredDocument clusteredDocument = new ClusteredDocument();

        @Override
        public void map(LongWritable key, TupleWritable value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
            int clusterId = ((IntWritable) value.get(1)).get();
            //clusteredDocument.setClusterId(clusterId);

            Text postWritable = (Text) value.get(0);
            Text postKeyWords = new Text("Cluster Id: " + clusterId + " Content: " + postWritable.toString());

            //clusteredDocument.setDocumentTitle(postWritable.getTitle());
            //clusteredDocument.setDocumentContent(postWritable.getContent());
            //output.collect(key, clusteredDocument);
            output.collect(key, postKeyWords);
        }
    }
}
