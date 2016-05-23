/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
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
import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 *
 * @author cuent
 */
public class KODAWriter {

    private static KODAWriter instanceWriter = new KODAWriter();
    private final String URL_TRANSLATE_ES_EN = "http://190.15.141.85:8080/marmottatest/pubman/translate?";

    private KODAWriter() {
    }

    public static KODAWriter getWriteSequenceFile() {
        return instanceWriter;
    }

    public void writeClusterVector(String pathVectorFile, String pathToSave) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(pathVectorFile), conf);

        File file = new File(pathToSave + "clusters.csv");
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
                if (valueElement != 0.0) {
                    bw.write((valueElement * 100) + ",");
                } else {
                    bw.write(new Double(1.0) + ",");
                }
            }
            bw.write("\n");
        }
        reader.close();
        bw.flush();
        bw.close();
    }

    public void disjoin(String pathOriginalFile, boolean translate) throws FileNotFoundException, IOException {
        FileReader filereader = new FileReader(new File(pathOriginalFile));
        BufferedReader br = new BufferedReader(filereader);
        HttpClient httpClient = HttpClients.createDefault();

        BufferedWriter outAuthors = new BufferedWriter(new FileWriter(new File("mahout-base/original/authors.csv")));
        BufferedWriter outKeywords = new BufferedWriter(new FileWriter(new File("mahout-base/original/keywords.csv")));

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
                                System.out.println(e);
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

    public void writeVector(String pathVectorFile, String pathToSave) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, new Path(pathVectorFile), conf);

        File file = new File(pathToSave + "tfidf.csv");
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
                if (valueElement != 0.0) {
                    bw.write((valueElement * 100) + ",");
                } else {
                    bw.write(new Double(1.0) + ",");
                }
            }
            bw.write("\n");
        }
        reader.close();
        bw.flush();
        bw.close();
    }

}
