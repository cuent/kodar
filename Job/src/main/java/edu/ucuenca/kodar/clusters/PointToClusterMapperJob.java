package edu.ucuenca.kodar.clusters;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
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
 */
class PointToClusterMapper extends Mapper<IntWritable, WeightedVectorWritable, LongWritable, IntWritable> {

    private LongWritable pointName = new LongWritable();

    @Override
    protected void map(IntWritable clusterId, WeightedVectorWritable point, Mapper.Context context) throws IOException, InterruptedException {
        NamedVector namedVector;
        if (point.getVector() instanceof NamedVector) {
            namedVector = (NamedVector) point.getVector();
        } else {
            throw new RuntimeException("Cannot output point name, point is not a NamedVector");
        }

        pointName.set(Long.valueOf(namedVector.getName()));

        context.write(pointName, clusterId);
    }
}
