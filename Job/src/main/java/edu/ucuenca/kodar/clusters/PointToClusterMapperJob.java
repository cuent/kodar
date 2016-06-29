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

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

public class PointToClusterMapperJob extends Configured {

    private Path clusteredPointsPath;
    private Path pointsToClusterPath;

    public PointToClusterMapperJob(Path clusteredPointsPath, Path pointsToClusterPath) {
        this.clusteredPointsPath = clusteredPointsPath;
        this.pointsToClusterPath = pointsToClusterPath;
    }

    public void mapPointsToClusters() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf();

        Job job = new Job(configuration, PointToClusterMapperJob.class.getSimpleName());
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setJarByClass(PointToClusterMapperJob.class);

        SequenceFileInputFormat.addInputPath(job, clusteredPointsPath);
        SequenceFileOutputFormat.setOutputPath(job, pointsToClusterPath);

        job.setMapperClass(PointToClusterMapper.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.waitForCompletion(true);
    }
}

/**
 * Maps the name of a point to the id of the cluster, sorted by point name in
 * ascending order.
 *
 * The clusteredPoints directory contains the final mapping from cluster ID to
 * document ID.
 */
class PointToClusterMapper extends Mapper<IntWritable, WeightedVectorWritable, LongWritable, IntWritable> {

    private LongWritable documentId = new LongWritable();

    @Override
    protected void map(IntWritable clusterId, WeightedVectorWritable point, Mapper.Context context) throws IOException, InterruptedException {
        NamedVector namedVector;
        if (point.getVector() instanceof NamedVector) {
            namedVector = (NamedVector) point.getVector();
        } else {
            throw new RuntimeException("Cannot output point name, point is not a NamedVector");
        }

        documentId.set(Long.valueOf(namedVector.getName()));

        context.write(documentId, clusterId);
    }
}
