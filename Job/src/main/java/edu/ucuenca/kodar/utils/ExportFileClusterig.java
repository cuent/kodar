/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ucuenca.kodar.utils;

import com.google.gson.Gson;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.vocabulary.RDFS;

/**
 *
 * @author cuent
 */
public class ExportFileClusterig {

    private static ExportFileClusterig instanceExport = new ExportFileClusterig();
    private static final String delimiter = "2db5c8";
    private static final String kw1 = "Cluster Id: ",
            kw2 = "Content: ", kw3 = " Author: ", kw6 = " Title: ",
            kw4 = " URI_A: ", kw5 = " URI_P: ";

    private ExportFileClusterig() {
    }

    public static ExportFileClusterig getExportFile() {
        return instanceExport;
    }

    public void writeResultFileCSV(String pathFileInput, String pathToWrite) throws IOException {
        Path path = new Path(pathFileInput);

        String newline = System.getProperty("line.separator");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        Text v = new Text();

        File file = new File(pathToWrite);
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);

        if (!file.exists()) {
            file.createNewFile();
        }

        bw.write("clusterId,label,author,kw,title,uriAuthor,uriPublication" + newline);
        while (reader.next(k, v)) {
            String[] values = v.toString().split(delimiter);
            for (String value : values) {
                String cluster = value.substring(value.indexOf(kw1) + kw1.length(), value.indexOf(kw2));
                String kw = value.substring(value.indexOf(kw2) + kw2.length(), value.indexOf(kw3));
                String author = value.substring(value.indexOf(kw3) + kw3.length(), value.indexOf(kw4));
                String uriA = value.substring(value.indexOf(kw4) + kw4.length(), value.indexOf(kw5));
                String uriP = value.substring(value.indexOf(kw5) + kw5.length(), value.indexOf(kw6));
                String title = value.substring(value.indexOf(kw6) + kw6.length());
                bw.write(cluster + "," + k.toString() + "," + author + "," + kw + "," + title + "," + uriA + "," + uriP);
                bw.write(newline);
            }

        }

        System.out.println("File written in " + pathToWrite);
        bw.close();
    }

    static String clusterURI = "http://ucuenca.edu.ec/resource/cluster";

    public void writeResultFileRDF(String pathFileInput, String pathToWrite) throws IOException {
        //create an empty model
        Model model = ModelFactory.createDefaultModel();
        //property foaf:publication
        Property foafPublication = model.createProperty("http://xmlns.com/foaf/0.1/publications");
        //property hasPerson
        Property hasPerson = model.createProperty("http://ucuenca.edu.ec/ontology#hasPerson");

        Path path = new Path(pathFileInput);

        String newline = System.getProperty("line.separator");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        Text v = new Text();

        File file = new File(pathToWrite);
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);

        if (!file.exists()) {
            file.createNewFile();
        }

        String cluster = "", uriA = "", uriP = "";

        while (reader.next(k, v)) {
            String[] values = v.toString().split(delimiter);
            for (String value : values) {
                cluster = value.substring(value.indexOf(kw1) + kw1.length(), value.indexOf(kw2)).trim();
                uriA = value.substring(value.indexOf(kw4) + kw4.length(), value.indexOf(kw5));//.replace(",", "");
                uriP = value.substring(value.indexOf(kw5) + kw5.length(), value.indexOf(kw6));//.replace(",", "");

                if (!uriA.trim().equals("") && !uriP.trim().equals("")) {
                    //uriA = URIUtil.encodeQuery(uriA);
                    //uriP = URIUtil.encodeQuery(uriP);

                    Resource r = model.createResource(clusterURI + cluster);
                    r.addProperty(foafPublication, model.createResource(uriP));
                    r.addProperty(RDFS.label, k.toString());
                    model.createResource(uriP).addProperty(hasPerson, model.createResource(uriA));
                }
            }
        }
        try {
            model.write(bw, "N-TRIPLE");
        } catch (org.apache.jena.shared.BadURIException ex) {
            System.out.println(cluster + newline + uriA + newline + uriP);
            System.err.print(ex);
        }
        System.out.println("File written in " + pathToWrite);
        bw.close();
    }

    public void writeResultFileJSON(String pathFileInput, String pathToWrite) throws IOException {
        //Library to serialitize to JSON 
        Gson gson = new Gson();

        Path path = new Path(pathFileInput);

        String newline = System.getProperty("line.separator");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, path, conf);
        Text k = new Text();
        Text v = new Text();

        File file = new File(pathToWrite);
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);

        if (!file.exists()) {
            file.createNewFile();
        }

        bw.write("[" + newline);
        while (reader.next(k, v)) {

            String[] values = v.toString().split(delimiter);
            for (String value : values) {
                String cluster = value.substring(value.indexOf(kw1) + kw1.length(), value.indexOf(kw2));
                String kw = value.substring(value.indexOf(kw2) + kw2.length(), value.indexOf(kw3));
                String author = value.substring(value.indexOf(kw3) + kw3.length(), value.indexOf(kw4));
                String uriA = value.substring(value.indexOf(kw4) + kw4.length(), value.indexOf(kw5));
                String uriP = value.substring(value.indexOf(kw5) + kw5.length(), value.indexOf(kw6));
                String title = value.substring(value.indexOf(kw6) + kw6.length());

                Template template = new Template(k.toString(), kw, author, title, uriA, uriP);

                String json = gson.toJson(template);

                bw.write(json + "," + newline);
            }

        }
        bw.write("]");

        System.out.println("File written in in " + pathToWrite);
        bw.close();
    }
}
