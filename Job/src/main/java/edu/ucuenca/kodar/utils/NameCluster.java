/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

import edu.uc.mahout.base.topicmodel.Tagger;
import edu.ucuenca.kodar.utils.nlp.Category;
import java.io.IOException;
import net.didion.jwnl.JWNLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.mahout.common.HadoopUtil;

/**
 *
 * @author cuent
 */
public class NameCluster {

    private Configuration conf;
    private static final String delimiter = "2db5c8", escapeContent = " Content: ", escapeAuthor = " Author:",
            escapeTitle = " Title: ", espapeURIA = " URI_A: ";

    public void execute(String pathFileInput) throws IOException, JWNLException, Exception {
        Path path = new Path(pathFileInput);

        FileSystem fs = FileSystem.get(conf);
        Path temp = new Path("mahout-base/topmodel/temp/", "part000");
        Path namedClusters = new Path("mahout-base/", "named-clusters");
        HadoopUtil.delete(conf, namedClusters);

        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        LongWritable k = new LongWritable();
        Text v = new Text();
        String document = "", kws, title;

        SequenceFile.Writer writeCluster = new SequenceFile.Writer(fs, conf, namedClusters, Text.class, Text.class);

        System.out.println("Reading: " + pathFileInput);
        while (reader.next(k, v)) {
            //for (int i = 0; i < 2; i++) {

            reader.next(k, v);
            String[] values = v.toString().split(delimiter);
            int id = 0;

            SequenceFile.Writer writer = new SequenceFile.Writer(fs, conf, temp, Text.class, Text.class);
            for (String value : values) {
                kws = value.substring(value.indexOf(escapeContent) + escapeContent.length(),
                        value.indexOf(escapeAuthor));
                title = value.substring(value.indexOf(escapeTitle) + escapeTitle.length());
                Category c = new Category(kws);
                c.populate();
                document = title + "\n" + kws + "\n" + c.toString();
                id++;
                writer.append(new Text(String.valueOf(id)), new Text(document));
            }
            writer.close();
            Tagger tagger = new Tagger();
            String label = tagger.tag(temp.toString());
            writeCluster.append(new Text(label), v);
            HadoopUtil.delete(conf, temp);
            //}
        }
        writeCluster.close();
    }

    public Configuration getConf() {
        return conf;
    }

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

}
