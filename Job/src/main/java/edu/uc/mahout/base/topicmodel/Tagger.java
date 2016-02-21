/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.uc.mahout.base.topicmodel;

import edu.ucuenca.kodar.clusters.MahoutController;
import edu.ucuenca.kodar.utils.KODAReader;
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

    public String tag(String inputPath) throws IOException, Exception {
        KODAReader mr = KODAReader.getReadSequenceFile();
        MahoutController base = new MahoutController();

        //Hadoop Configuration
        Configuration conf = new Configuration();

        //base.seqDirectoryToText(conf, "mahout-base/original/labels.csv", "mahout-base/topmodel/output");
        //mr.readTT(inputPath);
        //Generate Sparse Vectors
        String[] seq2sparse = new String[]{
            "-i", inputPath,
            "-o", "mahout-base/topmodel/sparse",
            "--maxDFPercent", "60",
            "-n", "2",
            "-ng", "2",
            "--weight", "TF",
            "-nv",
            "-seq",
            "-ow"
        };
        base.seq2Sparse(conf, seq2sparse);
//        mr.readIL("mahout-base/topmodel/sparse/df-count/part-r-00000");
//        mr.readTI("mahout-base/topmodel/sparse/dictionary.file-0");
//        mr.readIL("mahout-base/topmodel/sparse/frequency.file-0");
//        mr.readTV("mahout-base/topmodel/sparse/tf-vectors/part-r-00000");
//        mr.readTS("mahout-base/topmodel/sparse/tokenized-documents/part-m-00000");
        //mr.readTL("mahout-base/topmodel/sparse/wordcount/part-r-00000");

        //Convert to <SequenceFile,SequenceFile>
        String[] rowIdArgs = new String[]{
            "-i", "mahout-base/topmodel/sparse/tf-vectors",
            "-o", "mahout-base/topmodel/convert"
        };
        base.rowId(conf, rowIdArgs);

        Path path_cvb = new Path("mahout-base/topmodel/cvb");
        HadoopUtil.delete(conf, path_cvb);
        //Run collapse variational bayes algorithm
        String[] cvb = new String[]{
            "-i", "mahout-base/topmodel/convert/matrix",
            "-dict", "mahout-base/topmodel/sparse/dictionary.file-*",
            "-o", "mahout-base/topmodel/cvb/topic-term",
            "-dt", "mahout-base/topmodel/cvb/doc-topic/topic-model-cvb",
            "-mt", "mahout-base/topmodel/cvb",
            "-k", "1",
            "-x", "20",
            "-ow"
        };
        base.collapsedVariationalBayes(conf, cvb);

        String[] vectorDump = new String[]{
            "-i", "mahout-base/topmodel/cvb/topic-term/",
            "-o", "mahout-base/topmodel/vectorDump",
            "-d", "mahout-base/topmodel/sparse/dictionary.file-0",
            "-vs", "1",
            "-dt", "sequencefile",
            "-p", "true",
            "-sort", "true"
        };
        base.vectorDump(vectorDump);

        //Read label from text file
        BufferedReader br = new BufferedReader(new FileReader("mahout-base/topmodel/vectorDump"));
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
