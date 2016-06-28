/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.uc.mahout.base.topicmodel;

import edu.ucuenca.kodar.clusters.Controller;
import edu.ucuenca.kodar.clusters.ControllerImpl;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.mahout.common.HadoopUtil;

/**
 *
 * @author cuent
 */
public class Tagger {

    private static final String BASE_PATH = System.getProperty("user.dir") + "/";

    public String tag(String inputPath) throws IOException, Exception {
        //Hadoop Configuration
        Configuration conf = new Configuration();

        Controller base = new ControllerImpl(conf);

        //Generate Sparse Vectors
        String[] seq2sparse = new String[]{
            "-i", inputPath,
            "-o", BASE_PATH + "target/kodar_home/topmodel/sparse",
            "--maxDFPercent", "60",
            "-n", "2",
            "-ng", "2",
            "--weight", "TF",
            "-nv",
            "-seq",
            "-ow"
        };
        base.seq2Sparse(seq2sparse);

        //Convert to <SequenceFile,SequenceFile>
        String[] rowIdArgs = new String[]{
            "-i", BASE_PATH + "target/kodar_home/topmodel/sparse/tf-vectors",
            "-o", BASE_PATH + "target/kodar_home/topmodel/convert"
        };
        base.rowId(rowIdArgs);

        Path path_cvb = new Path(BASE_PATH + "target/kodar_home/topmodel/cvb");
        HadoopUtil.delete(conf, path_cvb);
        //Run collapse variational bayes algorithm
        String[] cvb = new String[]{
            "-i", BASE_PATH + "target/kodar_home/topmodel/convert/matrix",
            "-dict", BASE_PATH + "target/kodar_home/topmodel/sparse/dictionary.file-*",
            "-o", BASE_PATH + "target/kodar_home/topmodel/cvb/topic-term",
            "-dt", BASE_PATH + "target/kodar_home/topmodel/cvb/doc-topic/topic-model-cvb",
            "-mt", BASE_PATH + "target/kodar_home/topmodel/cvb",
            "-k", "1",
            "-x", "20",
            "-ow"
        };
        base.collapsedVariationalBayes(cvb);

        String[] vectorDump = new String[]{
            "-i", BASE_PATH + "target/kodar_home/topmodel/cvb/topic-term/",
            "-o", BASE_PATH + "target/kodar_home/topmodel/vectorDump",
            "-d", BASE_PATH + "target/kodar_home/topmodel/sparse/dictionary.file-0",
            "-vs", "1",
            "-dt", "sequencefile",
            "-p", "true",
            "-sort", "true"
        };
        base.vectorDump(vectorDump);

        //Read label from text file
        BufferedReader br = new BufferedReader(new FileReader(BASE_PATH + "target/kodar_home/topmodel/vectorDump"));
        String line = br.readLine();
        if (line != null) {
            line = line.substring(line.indexOf("{") + 1, line.indexOf(":"));
        } else {
            line = "No Label";
        }
        br.close();

        return line;
    }
}
