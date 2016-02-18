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
package edu.ucuenca.kodar.webservice;

import edu.ucuenca.kodar.clusters.Execute;
import edu.ucuenca.kodar.utils.Script;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.UriInfo;
import javax.ws.rs.PathParam;
import javax.ws.rs.Consumes;
import javax.ws.rs.Produces;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

/**
 * REST Web Service for Clustering researchers. You could clustering a
 * clustering job from an input file and download files in different formats
 * like rdf, csv and json. Web Service is called KODAR. It's an acronym for
 * Knowledge Of Discovery Areas Research. Words had been disordered until get
 * KODAR.
 *
 * @author Xavier Sumba
 */
@Path("clustering")
public class ClusteringResource {

    private final static Logger logger = Logger.getLogger(ClusteringResource.class.getName());
    private final static String BASEPATH = "/Users/cuent/NetBeansProjects/KODAR/Job/mahout-base/";
    @Context
    private UriInfo context;

    public ClusteringResource() {
    }

    /**
     * Retrieves a file based on the extension.
     *
     * @param typeFile
     * @return an instance of String or OutputStream
     */
    @GET
    @Path("download/{file}")
    @Produces(MediaType.APPLICATION_OCTET_STREAM)
    public Response getFile(@PathParam("file") String typeFile) {
        File file = null;
        logger.info(String.format("Looking for: '%s' extention file.", typeFile));

        switch (typeFile) {
            case "rdf":
                file = new File(BASEPATH + "final.rdf");
                break;
            case "csv":
                file = new File(BASEPATH + "final.csv");
                break;
            case "json":
                file = new File(BASEPATH + "final.json");
                break;
        }

        if (file != null && file.exists()) {
            logger.info(String.format("File has been sent sucessfully: %s", file.getAbsoluteFile()));
            return Response.ok(file, MediaType.APPLICATION_OCTET_STREAM)
                    .header("Content-Disposition", "attachment; filename=\"" + file.getName() + "\"")
                    .build();
        } else {
            logger.log(Level.WARNING, String.format("Couldn't find the file with extension: '%s'", typeFile));
            return Response.status(404).entity("File extension not found: " + typeFile)
                    .type("text/plain").build();
        }

    }

    /**
     * Execute Clustering Job to discover knowledge areas and similar
     * researches.
     *
     * @param inputStreamAuthors
     * @return
     */
    @POST
    @Path("execute")
    @Consumes(MediaType.APPLICATION_OCTET_STREAM)
    public Response clusteringExecution(InputStream inputStreamAuthors) throws Exception {
        String pathAuthors1 = BASEPATH + "original/authors1.csv";
        String pathAuthors = BASEPATH + "original/authors.csv";
        writeToFile(inputStreamAuthors, pathAuthors1);

        String pathKeywords = BASEPATH + "original/keywords.csv";

        String outputA = String.format("File uploaded to: %s", pathAuthors);
        logger.log(Level.INFO, outputA);
        String outputK = String.format("File uploaded to: %s", pathKeywords);
        logger.log(Level.INFO, outputK);

        //Clustering c = new Clustering(pathAuthors, pathKeywords);
        //c.run();
        String args[] = {pathAuthors, pathKeywords};
        //Execute.main(args);
        Script s = new Script();
        s.execute();
        return Response.status(200).entity(outputA + "\n" + outputK).build();
    }

    // save uploaded file to new location
    private void writeToFile(InputStream uploadedInputStream,
            String uploadedFileLocation) {

        try {
            OutputStream out = new FileOutputStream(new File(uploadedFileLocation));
            int read = 0;
            byte[] bytes = new byte[1024];

            out = new FileOutputStream(new File(uploadedFileLocation));
            while ((read = uploadedInputStream.read(bytes)) != -1) {
                out.write(bytes, 0, read);
            }
            out.flush();
            out.close();
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Found error when trying to write file in filesystem", e);
        }

    }
}
