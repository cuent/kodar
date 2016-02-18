/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.uc.mahout.base.exportJSON;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 *
 * @author cuent
 */
public class export {

    protected static HashMap<Integer, List<Researcher>> values = new HashMap<>();

    public static void main(String[] args) throws IOException {
        read();
        write();
    }

    public static void read() throws FileNotFoundException, IOException {
        BufferedReader br = new BufferedReader(new FileReader("/Users/cuent/Downloads/final.tsv"));
        String line = br.readLine(); //Cabecera del CSV o TSV

        List<Researcher> l = null;

        while ((line = br.readLine()) != null) {
            String[] field = line.split("\t");
            int cluster = Integer.parseInt(field[0]);

            if (!values.containsKey(cluster)) {
                l = new ArrayList();
            } else {
                l = values.get(cluster);
            }

            l.add(new Researcher(field[1], field[2]));
            values.put(cluster, l);
        }

        if (br != null) {
            br.close();
        }
    }

    public static void write() throws IOException {
        File file = new File("/Users/cuent/Desktop/file.json");
        String newline = System.getProperty("line.separator");

        if (!file.exists()) {
            file.createNewFile();
        }

        FileWriter fw = new FileWriter(file.getAbsoluteFile());
        BufferedWriter bw = new BufferedWriter(fw);

        bw.write("[" + newline);
        for (Integer key : values.keySet()) {
            bw.write("\t{" + newline);
            bw.write("\t\t\"cluster\":" + key + "," + newline);
            List<Researcher> l = values.get(key);
            String researches = "";
            for (int i = 0; i < l.size(); i++) {
                Researcher r = l.get(i);
                if (i == l.size() - 1) {
                    researches += "{\"author\":\"" + r.getAuthor().replaceAll("\"", "") + "\",\"keyword\":\"" + r.getKeyword().replace("\"", "") + "\"}";
                } else {
                    researches += "{\"author\":\"" + r.getAuthor().replaceAll("\"", "") + "\",\"keyword\":\"" + r.getKeyword().replace("\"", "") + "\"},";
                }
            }
            bw.write("\t\t\"members\": [" + researches + "]");
            bw.write("\t}," + newline);

        }
        bw.write("]" + newline);

        bw.close();
    }
}
