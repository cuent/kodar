package edu.uc.mahout.base.topicmodel;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.mahout.clustering.classify.WeightedVectorWritable;
import org.apache.mahout.math.NamedVector;

public class SortMapperJob extends Configured {

    private Path clusteredPointsPath;
    private Path pointsToClusterPath;

    public SortMapperJob(Path clusteredPointsPath, Path pointsToClusterPath) {
        this.clusteredPointsPath = clusteredPointsPath;
        this.pointsToClusterPath = pointsToClusterPath;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = getConf();

        Job job = new Job(configuration, SortMapperJob.class.getSimpleName());
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setJarByClass(SortMapperJob.class);

        SequenceFileInputFormat.addInputPath(job, clusteredPointsPath);
        SequenceFileOutputFormat.setOutputPath(job, pointsToClusterPath);

        job.setMapperClass(ClusterMapper.class);
        job.setReducerClass(ClusterReducer.class);
        job.setNumReduceTasks(1);

        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(Text.class);

        job.waitForCompletion(true);
    }
}

/**
 *
 *
 */
class ClusterMapper extends Mapper<LongWritable, Text, LongWritable, Text> {

    private LongWritable idCluster = new LongWritable();
    private Text content = new Text();

    @Override
    protected void map(LongWritable id, Text text, Mapper.Context context) throws IOException, InterruptedException {

        String information = text.toString();
        long idClusterLong = Long.parseLong(information.substring("Cluster Id: ".length(),
                information.indexOf(" Content: ")));
        idCluster.set(idClusterLong);
        content.set(information);

        context.write(idCluster, content);
    }
}

class ClusterReducer extends Reducer<LongWritable, Text, LongWritable, Text> {

    private Text contentAux;
    private Text content;
    private static final String delimiter = "2db5c8";

    @Override
    protected void reduce(LongWritable clusterId, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder buildContent = new StringBuilder();

        Iterator<Text> it = values.iterator();

        while (it.hasNext()) {
            content = it.next();
            if (it.hasNext()) {
                buildContent.append(content.toString() + delimiter);
            } else {
                buildContent.append(content.toString());
            }
        }

        content = new Text(buildContent.toString());
        context.write(clusterId, content);
    }
}
