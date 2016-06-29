/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URLEncoder;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.mahout.clustering.classify.WeightedPropertyVectorWritable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

/**
 *
 * @author cuent
 */
public class Writer {

    private static Writer instanceWriter = new Writer();
    private final String URL_TRANSLATE_ES_EN = "http://190.15.141.85:8080/marmottatest/pubman/translate?";

    private Writer() {
    }

    public static Writer getWriteSequenceFile() {
        return instanceWriter;
    }

    public void writeClusterVector(Path pathVectorFile, Path pathToSave) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, pathVectorFile, conf);

        File file = new File(pathToSave.toString(), "clusters.csv");
        FileWriter fr = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fr);

        IntWritable key = new IntWritable();
        WeightedPropertyVectorWritable value = new WeightedPropertyVectorWritable();
        double valueElement = 0;
        while (reader.next(key, value)) {
            Iterator it = value.getVector().all().iterator();
            bw.write(key.toString() + ",");
            while (it.hasNext()) {
                Vector.Element element = (Vector.Element) it.next();
                valueElement = element.get();

                if (it.hasNext()) {
                    bw.write(valueElement + ",");
                } else {
                    bw.write(valueElement + "");
                }
            }
            bw.write("\n");
        }
        reader.close();
        bw.flush();
        bw.close();
    }

    public void disjoin(File inputFile, File outputPath, boolean translate) throws FileNotFoundException, IOException {
        FileReader filereader = new FileReader(inputFile);
        BufferedReader br = new BufferedReader(filereader);
        HttpClient httpClient = HttpClients.createDefault();

        if (!outputPath.exists()) {
            outputPath.mkdir();
        }

        BufferedWriter outAuthors = new BufferedWriter(new FileWriter(new File(outputPath, "authors.csv")));
        BufferedWriter outKeywords = new BufferedWriter(new FileWriter(new File(outputPath, "keywords.csv")));

        String line = br.readLine();
        String newline = System.getProperty("line.separator");
        int id = 0;
        String keywords;

        while ((line = br.readLine()) != null) {
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)", -1);

            outKeywords.write(String.valueOf(id) + ",");
            outAuthors.write(String.valueOf(id) + ",");

            for (int i = 0; i < fields.length; i++) {
                if (i == 4) {
                    keywords = fields[i];
                    if (translate) {
                        String kwEncoded = URLEncoder.encode(keywords, "UTF-8");
                        HttpPost post = new HttpPost(URL_TRANSLATE_ES_EN + "totranslate=" + kwEncoded);

                        post.addHeader("Content-Type", "application/x-www-form-urlencoded");
                        post.addHeader("Accept", "application/ld+json");

                        HttpResponse response = httpClient.execute(post);
                        HttpEntity entity = response.getEntity();

                        if (entity != null) {
                            BufferedReader reader = null;
                            try {
                                reader = new BufferedReader(new InputStreamReader(entity.getContent(), "UTF-8"));

                                String jsonResult = reader.readLine();
                                String kwTranslated = ((JsonObject) new JsonParser().parse(jsonResult)).get("result").toString();

                                outKeywords.write(kwTranslated);
                            } catch (Exception e) {
                                outKeywords.write(keywords);
                            } finally {
                                reader.close();
                            }
                        }
                    } else {
                        outKeywords.write(keywords);
                    }
                } else {
                    outAuthors.write(fields[i]);
                }
                if (i != (fields.length - 1)) {
                    outAuthors.write(",");
                }
            }

            outKeywords.write(newline);
            outAuthors.write(newline);
            id++;
        }

        outAuthors.flush();
        outKeywords.flush();
        outAuthors.close();
        outKeywords.close();
    }

    public void writeVector(Path pathVectorFile, Path pathToSave) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, pathVectorFile, conf);

        File file = new File(pathToSave.toString(), "tfidf.csv");
        FileWriter fr = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fr);

        Text key = new Text();
        VectorWritable value = new VectorWritable();
        double valueElement = 0;
        while (reader.next(key, value)) {
            Iterator it = value.get().all().iterator();
            while (it.hasNext()) {
                Vector.Element element = (Vector.Element) it.next();
                valueElement = element.get();
                if (it.hasNext()) {
                    bw.write(valueElement + ",");
                } else {
                    bw.write(valueElement + "");
                }
            }
            bw.write("\n");
        }
        reader.close();
        bw.flush();
        bw.close();
    }

}
