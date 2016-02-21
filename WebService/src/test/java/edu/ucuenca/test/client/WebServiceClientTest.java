/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.ucuenca.test.client;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.net.URLConnection;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * It's a Web Service Client to consume KODAR REST Service.
 *
 * @author Xavier Sumba
 */
public class WebServiceClientTest {

    private final static Logger logger = Logger.getLogger(WebServiceClientTest.class.getName());
    private final String filename = "clustering";
    private final String URL = "http://localhost:8080/WebService/clustering/";

    /**
     * Clustering dataset in filePath.
     *
     * @param filePath - dataset to be clustered
     */
    public void execute(String filePath) {
        try {

            // Read file from filesystem
            InputStream inmputStream = new FileInputStream(filePath);
            InputStreamReader streamReader = new InputStreamReader(inmputStream);
            BufferedReader br = new BufferedReader(streamReader);
            String line;

            // Pass File Data to REST Service and execute Clustering
            URL url = new URL(URL + "execute/");
            URLConnection connection = url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/octet-stream");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            OutputStreamWriter out = new OutputStreamWriter(connection.getOutputStream());
            while ((line = br.readLine()) != null) {
                out.write(line);
                out.write(System.getProperty("line.separator"));
            }
            out.flush();
            out.close();

            // connection.getInputStream() gives stream message of success
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String message;
            while ((message = in.readLine()) != null) {
                logger.info(message);
            }
            logger.info("KODA REST Service Invoked Successfully..");
            in.close();

            br.close();
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while calling Crunchify REST Service", e);
        }
    }

    /**
     * Download the type file specified if it is found in the pathToDownload.
     *
     * @param typeFile - extension of the file to Download
     * @param pathToDownload - Path to download the file
     */
    public void getFile(String typeFile, String pathToDownload) {
        try {
            // Pass extention File to get stream from REST Service
            URL url = new URL(URL + "download/" + typeFile);
            URLConnection connection = url.openConnection();
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/octet-stream");
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));

            pathToDownload += (pathToDownload.substring(pathToDownload.length() - 1).equals("/")) ? "" : "/";
            File file = new File(pathToDownload + filename + "." + typeFile);
            FileWriter fr = new FileWriter(file);
            BufferedWriter bw = new BufferedWriter(fr);

            // Write in the buffer line by line in pathToDownload
            String line;
            while ((line = br.readLine()) != null) {
                bw.write(line);
            }

            br.close();
            bw.flush();
            bw.close();

            logger.log(Level.INFO, String.format("File sucessfully downloaded in %s", file.getAbsolutePath()));
            logger.log(Level.INFO, "KODAR REST Service Invoked Sucessfully.");
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while calling KODAR REST Service", e);
        }
    }

}
