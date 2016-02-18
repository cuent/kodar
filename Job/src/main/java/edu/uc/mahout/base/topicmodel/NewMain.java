/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.uc.mahout.base.topicmodel;

import edu.ucuenca.kodar.utils.NameCluster;
import edu.ucuenca.kodar.utils.KODAWriter;
import edu.ucuenca.kodar.utils.KODAReader;
import java.io.IOException;
import net.didion.jwnl.JWNLException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

/**
 *
 * @author cuent
 */
public class NewMain {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, JWNLException, Exception {
        // TODO code application logic here
        Configuration conf = new Configuration();
        NameCluster reader = new NameCluster();

        Path clusters = new Path("mahout-base/result/part-00000");
        Path output = new Path("mahout-base/", "sort");

        HadoopUtil.delete(conf, output);

        SortMapperJob joinKwAuthors = new SortMapperJob(clusters,
                output);
        joinKwAuthors.setConf(conf);
        joinKwAuthors.run();

    }

}
